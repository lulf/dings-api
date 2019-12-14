/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package api

type Device struct {
	ID          string   `json:"device-id"`
	Enabled     bool     `json:"enabled"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Sensors     []string `json:"sensors,omitempty"`
}

type Event struct {
	DeviceId     string                 `json:"deviceId"`
	CreationTime int64                  `json:"creationTime"`
	Data         map[string]interface{} `json:"data"`
}
