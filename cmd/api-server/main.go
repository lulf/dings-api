/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"os"

	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/graphql-go/graphql"
	// "github.com/lulf/teig-event-sink/pkg/eventstore"
)

type deviceRegistryResponse struct {
	Devices []device `json:"devices"`
}

type device struct {
	ID          string   `json:"device-id"`
	Enabled     bool     `json:"enabled"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Sensors     []string `json:"sensors,omitempty"`
}

type deviceFetcherFunc func() ([]device, error)

func createSchema(deviceFetcher deviceFetcherFunc) graphql.Schema {
	var deviceType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Device",
			Fields: graphql.Fields{
				"id": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						d := p.Source.(device)
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

	var eventType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Event",
			Fields: graphql.Fields{
				"id": &graphql.Field{
					Type: graphql.String,
				},
				"deviceId": &graphql.Field{
					Type: graphql.String,
				},
				"CreationTime": &graphql.Field{
					Type: graphql.String,
				},
				"Payload": &graphql.Field{
					Type: graphql.String,
				},
			},
		},
	)

	var queryType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"deviceList": &graphql.Field{
					Type: graphql.NewList(deviceType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						data, err := deviceFetcher()
						return data, err
					},
				},
				"events": &graphql.Field{
					Type: graphql.NewList(eventType),
					/*
						Args: graphql.FieldConfigArgument {
							"since", &graphql
						},
					*/

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
		fmt.Printf("wrong result, unexpected errors: %v", result.Errors)
	}
	return result
}

func main() {
	var eventstoreAddr string
	var deviceRegistrationApi string
	var tenantId string
	var tenantPassword string
	flag.StringVar(&eventstoreAddr, "a", "amqp://127.0.0.1:5672", "Address of AMQP event store")
	flag.StringVar(&deviceRegistrationApi, "d", "https://manage.bosch-iot-hub.com/registration", "Device Registration API")
	flag.StringVar(&tenantId, "t", "", "Tenant ID")
	flag.StringVar(&tenantPassword, "p", "", "Tenant Password")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-a event_store_url] [-d device_registration_api_url] -t tenant_id -p password \n")
	}
	flag.Parse()

	username := fmt.Sprintf("device-registry@%s", tenantId)
	deviceRegistryClient := &http.Client{}

	schema := createSchema(func() ([]device, error) {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", deviceRegistrationApi, tenantId), nil)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(username, tenantPassword)

		resp, err := deviceRegistryClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		var result deviceRegistryResponse
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, err
		}
		return result.Devices, nil
	})

	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		result := executeQuery(r.URL.Query().Get("query"), schema)
		json.NewEncoder(w).Encode(result)
	})

	fmt.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", nil)
}

//Helper function to import json from file to map
func importJSONDataFromFile(fileName string, result interface{}) (isOK bool) {
	isOK = true
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Print("Error:", err)
		isOK = false
	}
	err = json.Unmarshal(content, result)
	if err != nil {
		isOK = false
		fmt.Print("Error:", err)
	}
	return
}
