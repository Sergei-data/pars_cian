package config

import (
	"os"
	"pp/pkg/cronitor"
)

type Config struct {
	Debug    bool
	Port     string
	Cronitor cronitor.Cronitor
}

var configInstance *Config

func Get() *Config {
	if configInstance == nil {
		configInstance = newConfig()
	}
	return configInstance
}

func newConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	debug := os.Getenv("DEBUG") == "true"

	if debug {
		return newFakeConfig(debug, port)
	}
	return newRealConfig(debug, port)
}

func newRealConfig(debug bool, port string) *Config {
	return &Config{
		Debug:    debug,
		Port:     port,
		Cronitor: cronitor.NewCronitor(),
	}
}

func newFakeConfig(debug bool, port string) *Config {
	return &Config{
		Debug:    debug,
		Port:     port,
		Cronitor: cronitor.NewFakeCronitor(),
	}
}
