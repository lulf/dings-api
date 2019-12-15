/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/lulf/teig-api/pkg/api"
)

type queryBody struct {
	Query string `json:"query"`
}

type deviceFetcherFunc func() ([]api.Device, error)
type eventFetcherFunc func(string, int) ([]api.Event, error)

func createSchema(deviceFetcher deviceFetcherFunc, eventFetcher eventFetcherFunc) graphql.Schema {
	var deviceType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Device",
			Fields: graphql.Fields{
				"id": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						d := p.Source.(api.Device)
						return d.ID, nil
					},
				},
				"enabled": &graphql.Field{
					Type: graphql.Boolean,
				},
				"name": &graphql.Field{
					Type: graphql.String,
				},
				"description": &graphql.Field{
					Type: graphql.String,
				},
				"sensors": &graphql.Field{
					Type: graphql.NewList(graphql.String),
				},
			},
		},
	)

	var eventDataType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Data",
			Fields: graphql.Fields{
				"motion": &graphql.Field{
					Type: graphql.Boolean,
				},
				"temperature": &graphql.Field{
					Type: graphql.Float,
				},
			},
		})

	var eventType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Event",
			Fields: graphql.Fields{
				"deviceId": &graphql.Field{
					Type: graphql.String,
				},
				"creationTime": &graphql.Field{
					Type: graphql.String,
				},
				"data": &graphql.Field{
					Type: eventDataType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						e := p.Source.(api.Event)
						return e.Data, nil
					},
				},
			},
		},
	)

	var queryType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"devices": &graphql.Field{
					Type: graphql.NewList(deviceType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						data, err := deviceFetcher()
						return data, err
					},
				},
				"events": &graphql.Field{
					Type: graphql.NewList(eventType),
					Args: graphql.FieldConfigArgument{
						"deviceId": &graphql.ArgumentConfig{
							Type: graphql.String,
						},
						"max": &graphql.ArgumentConfig{
							Type: graphql.Int,
						},
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						deviceId, ok := p.Args["deviceId"].(string)
						if ok {
							max, ok := p.Args["max"].(int)
							if ok {
								return eventFetcher(deviceId, max)
							} else {
								return eventFetcher(deviceId, 0)
							}
						}
						return nil, nil
					},
				},
			},
		})

	var schema, _ = graphql.NewSchema(
		graphql.SchemaConfig{
			Query: queryType,
		},
	)
	return schema
}

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		log.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}

func main() {
	var eventStoreUrl string
	var topic string
	var offset int64
	var deviceRegistryUrl string
	var username string
	var password string
	flag.StringVar(&eventStoreUrl, "a", "127.0.0.1:5672", "Address of AMQP event store")
	flag.StringVar(&deviceRegistryUrl, "d", "", "Device Registration API")
	flag.StringVar(&username, "u", "", "Device registry username")
	flag.StringVar(&password, "p", "", "Device registry password")
	flag.StringVar(&topic, "t", "events", "Event store topic")
	flag.Int64Var(&offset, "o", 0, "Event store offset")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-a event_store_url] [-d device_registry_url] -u username -p password \n")
	}
	flag.Parse()

	deviceRegistryClient := api.NewDeviceRegistryClient(deviceRegistryUrl, username, password)
	eventCache := api.NewEventCache(eventStoreUrl)

	go eventCache.Run(topic, offset)

	schema := createSchema(deviceRegistryClient.ListDevices, eventCache.ListEvents)
	http.HandleFunc("/graphql",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
			if r.Method == "POST" {
				body, err := ioutil.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var data queryBody
				err = json.Unmarshal(body, &data)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				result := executeQuery(data.Query, schema)
				json.NewEncoder(w).Encode(result)
			}
		})

	log.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", nil)
}
