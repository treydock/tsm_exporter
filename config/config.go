// Copyright 2020 Trey Dockendorf
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"os"
	"sync"

	yaml "gopkg.in/yaml.v3"
)

type Config struct {
	Targets map[string]*Target `yaml:"targets"`
}

type SafeConfig struct {
	sync.RWMutex
	C *Config
}

type Target struct {
	sync.Mutex
	Name                 string
	Servername           string            `yaml:"servername"`
	Id                   string            `yaml:"id"`
	Password             string            `yaml:"password"`
	LibraryName          string            `yaml:"library_name"`
	Schedules            []string          `yaml:"schedules"`
	ReplicationNodeNames []string          `yaml:"replication_node_names"`
	Collectors           []string          `yaml:"collectors,omitempty"`
	VolumeUsageMap       map[string]string `yaml:"volumeusage_map,omitempty"`
}

func (sc *SafeConfig) ReloadConfig(configFile string) error {
	var c = &Config{}
	yamlReader, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("Error reading config file %s: %s", configFile, err)
	}
	defer yamlReader.Close()
	decoder := yaml.NewDecoder(yamlReader)
	decoder.KnownFields(true)
	if err := decoder.Decode(c); err != nil {
		return fmt.Errorf("Error parsing config file %s: %s", configFile, err)
	}
	for key := range c.Targets {
		target := c.Targets[key]
		target.Name = key
		if target.Servername == "" {
			target.Servername = key
		}
		if target.Id == "" {
			return fmt.Errorf("Target %s must define 'id' value", key)
		}
		if target.Password == "" {
			return fmt.Errorf("Target %s must define 'password' value", key)
		}
		c.Targets[key] = target
	}
	sc.Lock()
	sc.C = c
	sc.Unlock()
	return nil
}
