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

type OrderData struct {
	OrderUID string `json:"order_uid"`
	// Другие поля модели
}

var (
	cache     = make(map[string]OrderData)
	cacheLock sync.RWMutex
)

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

func main() {
	// Открываем соединение с базой данных
	db, err := sql.Open("postgres", "user=username dbname=mydb sslmode=disable")
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Загружаем кэш из базы данных
	loadCacheFromDB(db)

	// Настройки для подключения к серверу NATS Streaming
	clusterID := "test-cluster"
	clientID := "your-client-id"
	stanURL := "nats://localhost:4222"

	// Создание подключения к серверу NATS Streaming
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(stanURL))
	if err != nil {
		log.Fatalf("Error connecting to NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Обработчик сообщений
	msgHandler := func(msg *stan.Msg) {
		var orderData OrderData

		// Распаковываем JSON-данные из сообщения
		err := json.Unmarshal(msg.Data, &orderData)
		if err != nil {
			log.Printf("Error decoding JSON: %v", err)
			return
		}

		// Кэшируем данные в памяти
		cacheLock.Lock()
		cache[orderData.OrderUID] = orderData
		cacheLock.Unlock()

		// Вставляем данные в базу данных
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
	defer sub.Unsubscribe()

	log.Printf("Subscribed to channel: %s", channel)

	// HTTP-обработчик для получения данных из кэша
	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		orderUID := r.URL.Query().Get("order_uid")

		// Чтение из кэша
		cacheLock.RLock()
		orderData, ok := cache[orderUID]
		cacheLock.RUnlock()

		if ok {
			// Если данные найдены в кэше, отправляем их клиенту
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(orderData)
		} else {
			// Если данных нет в кэше, возвращаем ошибку
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

	// Ожидание сигнала завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	log.Println("Shutting down...")
}
