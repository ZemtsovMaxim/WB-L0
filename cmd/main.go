package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

// OrderData - структура для хранения данных о заказе
type OrderData struct {
	OrderUID string `json:"order_uid"`
	// Добавьте другие поля из вашей модели данных
}

var (
	cache     map[string]OrderData
	cacheLock sync.RWMutex
	db        *sql.DB
	sc        stan.Conn
)

func main() {
	// Подключение к серверу NATS Streaming
	clusterID := "test-cluster" // Замените на фактический идентификатор вашего кластера
	clientID := "client-id"

	var err error
	sc, err = stan.Connect(clusterID, clientID, stan.NatsURL("nats://nats-streaming:4222"))
	if err != nil {
		log.Printf("Error connecting to NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Подключение к базе данных PostgreSQL
	pgURL := "postgres://My_user:1234554321@172.19.0.2:5432/postgres?sslmode=disable"
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Printf("Error connecting to the database: %v", err)
	}
	defer db.Close()
	time.Sleep(time.Second * 5)

	// Инициализация кэша
	cache = make(map[string]OrderData)

	// Загрузка кэша из базы данных
	loadCacheFromDB()
	time.Sleep(time.Second * 5)

	// Подписка на канал 172.19.0.2
	channel := "my_channel"
	sub, err := sc.Subscribe(channel, msgHandler, stan.DurableName("your-durable-name"))
	if err != nil {
		log.Printf("Error subscribing to channel: %v", err)
	}
	defer sub.Close()
	log.Printf("Subscribed to channel: %s", channel)

	// Ожидание сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	log.Println("Shutting down...")
}

// Обработчик сообщений NATS Streaming
func msgHandler(msg *stan.Msg) {
	var orderData OrderData

	err := json.Unmarshal(msg.Data, &orderData)
	if err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return
	}

	// Обновление кэша
	cacheLock.Lock()
	cache[orderData.OrderUID] = orderData
	cacheLock.Unlock()

	// Сохранение данных в базе данных
	_, err = db.Exec("INSERT INTO orders (order_uid, data) VALUES ($1, $2) ON CONFLICT (order_uid) DO UPDATE SET data = EXCLUDED.data", orderData.OrderUID, msg.Data)
	if err != nil {
		log.Printf("Error inserting data into the database: %v", err)
		return
	}

	log.Printf("Received, inserted, and cached a message: %s", msg.Data)
}

// Загрузка кэша из базы данных
func loadCacheFromDB() {
	rows, err := db.Query("SELECT order_uid, data FROM orders")
	if err != nil {
		log.Printf("Error querying data from the database: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var orderUID string
		var dataJSON []byte
		if err := rows.Scan(&orderUID, &dataJSON); err != nil {
			log.Printf("Error scanning data from the database: %v", err)
			continue
		}

		var orderData OrderData
		if err := json.Unmarshal(dataJSON, &orderData); err != nil {
			log.Printf("Error decoding JSON from the database: %v", err)
			continue
		}

		cacheLock.Lock()
		cache[orderUID] = orderData
		cacheLock.Unlock()
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over rows: %v", err)
	}
}
