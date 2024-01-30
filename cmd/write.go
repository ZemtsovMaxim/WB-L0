package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/stan.go"
)

func PublishDataToNATS() {
	// Подключение к серверу NATS Streaming
	clusterID := "test-cluster" // Замените на фактический идентификатор вашего кластера
	clientID := "client-id"

	sc, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Fatalf("Error connecting to NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Данные для публикации
	orderData := OrderData{
		OrderUID: "your-order-uid",
		// Замените на фактический идентификатор заказа
	}

	// Преобразование данных в JSON
	dataJSON, err := json.Marshal(orderData)
	if err != nil {
		log.Fatalf("Error encoding JSON: %v", err)
	}

	// Публикация данных в канал
	channel := "your-channel"
	err = sc.Publish(channel, dataJSON)
	if err != nil {
		log.Fatalf("Error publishing message: %v", err)
	}

	log.Printf("Published message to channel: %s", channel)
	time.Sleep(time.Second) // Подождем немного, чтобы убедиться, что сообщение успевает обработаться
}
