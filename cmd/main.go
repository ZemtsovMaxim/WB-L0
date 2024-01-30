package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
)

func main() {
	// Подключение к серверу NATS Streaming
	clusterID := "your-cluster-id" // Замените на фактический идентификатор вашего кластера
	clientID := "your-client-id"
	natsURL := os.Getenv("nats://localhost:4222")

	// Подключение к NATS Streaming
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatalf("Error connecting to NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Подключение к базе данных PostgreSQL
	pgURL := os.Getenv("POSTGRES_URL")
	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Инициализация кэша
	cache = make(map[string]OrderData)

	// Загрузка кэша из базы данных
	loadCacheFromDB(db)

	// Обработчик сообщений NATS Streaming
	msgHandler := func(msg *stan.Msg) {
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

	// Подписка на канал
	channel := "your-channel"
	sub, err := sc.Subscribe(channel, msgHandler, stan.DurableName("your-durable-name"))
	if err != nil {
		log.Fatalf("Error subscribing to channel: %v", err)
	}
	defer sub.Close()

	log.Printf("Subscribed to channel: %s", channel)

	// HTTP-обработчик
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		orderUID := r.URL.Query().Get("order_uid")

		cacheLock.RLock()
		orderData, ok := cache[orderUID]
		cacheLock.RUnlock()

		if ok {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(orderData)
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Order not found"))
		}
	})

	// Запуск HTTP-сервера
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Ожидание сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	log.Println("Shutting down...")
}

// Загрузка кэша из базы данных
func loadCacheFromDB(db *sql.DB) {
	rows, err := db.Query("SELECT order_uid, data FROM orders")
	if err != nil {
		log.Fatalf("Error querying data from the database: %v", err)
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
}
