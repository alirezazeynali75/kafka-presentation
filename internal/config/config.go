package config

import (
	"fmt"
	"log"

	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
)

type Configuration struct {
	AppName               string `env:"APP_NAME"  envDefault:"kafka-presentation"`
	Env                   string `env:"ENVIRONMENT" envDefault:"development"`
	Kafka									KafkaConfig
}

func Configure() (*Configuration, error) {
	err := godotenv.Load(".env")
	if err != nil {
		log.Print("can not find the .env file")
	}

	cfg := &Configuration{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing configuration error: %w", err)
	}

	return cfg, nil
}