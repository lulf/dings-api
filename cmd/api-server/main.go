/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/graphql-go/graphql"
	"pack.ag/amqp"
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
	Events      []event  `json:"events,omitempty"`
}

type event struct {
	DeviceId     string                 `json:"deviceId"`
	CreationTime int64                  `json:"creationTime"`
	Data         map[string]interface{} `json:"data"`
}

type eventCache struct {
	mutex sync.Mutex
	data  []event
}

type queryBody struct {
	Query string `json:"query"`
}

type deviceFetcherFunc func() ([]device, error)
type eventFetcherFunc func(string) ([]event, error)

func createSchema(deviceFetcher deviceFetcherFunc, eventFetcher eventFetcherFunc) graphql.Schema {
	var deviceType = graphql.NewObject(
		graphql.ObjectConfig{
			Name: "Device",
			Fields: graphql.Fields{
				"id": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						fmt.Println("Trying to resolve")
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
				"deviceId": &graphql.Field{
					Type: graphql.String,
				},
				"creationTime": &graphql.Field{
					Type: graphql.String,
				},
				"temperature": &graphql.Field{
					Type: graphql.Int,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						e := p.Source.(event)
						return e.Data["temperature"], nil
					},
				},
				"motion": &graphql.Field{
					Type: graphql.Boolean,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						e := p.Source.(event)
						return e.Data["motion"], nil
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
					},
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						deviceId, ok := p.Args["deviceId"].(string)
						if ok {
							return eventFetcher(deviceId)
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
	var deviceRegistrationApi string
	var tenantId string
	var tenantPassword string
	flag.StringVar(&eventStoreUrl, "a", "amqp://127.0.0.1:5672", "Address of AMQP event store")
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
	cache := eventCache{
		data: make([]event, 0),
	}

	go syncEventCache(eventStoreUrl, &cache)

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
		fmt.Println("Devices", result.Devices)
		return result.Devices, nil
	}, func(deviceId string) ([]event, error) {
		cache.mutex.Lock()
		defer cache.mutex.Unlock()
		var ret []event = make([]event, 0)
		for _, e := range cache.data {
			if e.DeviceId == deviceId {
				ret = append(ret, e)
			}
		}
		return ret, nil
	})

	http.HandleFunc("/graphql", BasicAuth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		if r.Method == "POST" {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fmt.Println("BODY: ", string(body))
			var data queryBody
			err = json.Unmarshal(body, &data)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			result := executeQuery(data.Query, schema)
			json.NewEncoder(w).Encode(result)
		}
	}, "test", "test", "teig"))

	log.Println("Now server is running on port 8080")
	http.ListenAndServe(":8080", nil)
}

func BasicAuth(handler http.HandlerFunc, username, password, realm string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		user, pass, ok := r.BasicAuth()

		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(username)) != 1 || subtle.ConstantTimeCompare([]byte(pass), []byte(password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+realm+`"`)
			w.WriteHeader(401)
			w.Write([]byte("Unauthorised.\n"))
			return
		}

		handler(w, r)
	}
}

func syncEventCache(eventStoreUrl string, cache *eventCache) error {
	client, err := amqp.Dial(eventStoreUrl)
	if err != nil {
		return err
	}

	session, err := client.NewSession()
	if err != nil {
		return err
	}

	receiver, err := session.NewReceiver(amqp.LinkSourceAddress("events"), amqp.LinkCredit(10)) //, amqp.LinkSelectorFilter("offset=0"))
	if err != nil {
		return err
	}

	log.Printf("Connected to event store %s", eventStoreUrl)

	for {
		msg, err := receiver.Receive(context.TODO())
		if err != nil {
			log.Fatal("Error reading message from AMQP:", err)
		}

		cache.mutex.Lock()

		var result event
		err = json.Unmarshal(msg.GetData(), &result)
		if err != nil {
			msg.Reject(nil)
			log.Print("Error decoding message:", err)
		} else {
			cache.data = append(cache.data, result)
			msg.Accept()
		}
		cache.mutex.Unlock()
	}
}
