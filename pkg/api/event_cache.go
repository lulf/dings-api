/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package api

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
)

type eventCache struct {
	receiver      electron.Receiver
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

func (cache *eventCache) Connect(topic string, offset int64) error {
	tcpConn, err := net.Dial("tcp", cache.eventStoreUrl)
	if err != nil {
		return err
	}
	amqpConn, err := electron.NewConnection(tcpConn, electron.ContainerId("teig-api"))

	props := map[amqp.Symbol]interface{}{"offset": offset}
	sopts := []electron.LinkOption{electron.Source(topic), electron.Filter(props)}
	r, err := amqpConn.Receiver(sopts...)
	if err != nil {
		return err
	}
	cache.receiver = r
	return nil
}

func (cache *eventCache) Run(done chan error) {
	log.Printf("Connected to event store %s", cache.eventStoreUrl)
	for {
		if rm, err := cache.receiver.Receive(); err == nil {
			msg := rm.Message
			var result Event
			err = json.Unmarshal([]byte(msg.Body().(amqp.Binary)), &result)
			if err != nil {
				rm.Reject()
				log.Println("Error decoding message:", err)
			} else {
				cache.mutex.Lock()
				cache.data = append(cache.data, result)
				cache.mutex.Unlock()
				rm.Accept()
			}
		} else if err == electron.Closed {
			done <- nil
			break
		} else {
			log.Println("receive error %v", err)
			done <- err
			break
		}
	}
}

func (cache *eventCache) ListEvents(deviceId string, max int, since int64) ([]Event, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	var ret []Event = make([]Event, 0)
	numValues := 0
	for _, e := range cache.data {
		if e.DeviceId == deviceId && e.CreationTime >= since {
			ret = append(ret, e)
			numValues += 1
			if max > 0 && numValues >= max {
				break
			}
		}
	}
	return ret, nil
}
