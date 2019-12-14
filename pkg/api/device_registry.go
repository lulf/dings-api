/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type deviceRegistryResponse struct {
	Devices []Device `json:"devices"`
}

type deviceRegistry struct {
	client   *http.Client
	url      string
	username string
	password string
}

func NewDeviceRegistryClient(url string, username string, password string) *deviceRegistry {
	return &deviceRegistry{
		client:   &http.Client{},
		url:      url,
		username: username,
		password: password,
	}
}

func (d *deviceRegistry) ListDevices() ([]Device, error) {
	req, err := http.NewRequest("GET", d.url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(d.username, d.password)

	resp, err := d.client.Do(req)
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
}
