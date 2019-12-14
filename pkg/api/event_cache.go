/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package api

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"pack.ag/amqp"
)

type eventCache struct {
	eventStoreUrl string
	mutex         sync.Mutex
	data          []Event
}

func NewEventCache(eventStoreUrl string) *eventCache {
	return &eventCache{
		eventStoreUrl: eventStoreUrl,
		data:          make([]Event, 0),
	}
}

func (cache *eventCache) Run() error {
	client, err := amqp.Dial(cache.eventStoreUrl)
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

	log.Printf("Connected to event store %s", cache.eventStoreUrl)

	for {
		msg, err := receiver.Receive(context.TODO())
		if err != nil {
			log.Fatal("Error reading message from AMQP:", err)
		}

		cache.mutex.Lock()

		var result Event
		err = json.Unmarshal(msg.GetData(), &result)
		if err != nil {
			msg.Reject(nil)
			log.Print("Error decoding message:", err)
		} else {
			cache.data = append(cache.data, result)
			log.Println("Cache contains", cache.data)
			msg.Accept()
		}
		cache.mutex.Unlock()
	}
}

func (cache *eventCache) ListEvents(deviceId string) ([]Event, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	var ret []Event = make([]Event, 0)
	for _, e := range cache.data {
		if e.DeviceId == deviceId {
			ret = append(ret, e)
		}
	}
	return ret, nil
}
