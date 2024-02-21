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
	"time"

	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

// OrderDB - структура для хранения данных о заказе в базе данных
type OrderDB struct {
	OrderUID  string `json:"order_uid"`
	OrderData string `json:"order_data"`
}

var (
	cache     map[string]OrderDB
	cacheLock sync.RWMutex
	db        *sql.DB
	sc        stan.Conn
)

func main() {
	// Подключение к серверу NATS Streaming
	clusterID := "test-cluster"
	clientID := "client-id"

	var err error
	sc, err = stan.Connect(clusterID, clientID, stan.NatsURL("nats://nats-streaming:4222"))
	if err != nil {
		log.Printf("Error connecting to NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Подключение к базе данных PostgreSQL
	pgURL := "postgres://My_user:1234554321@172.19.0.2:5432/My_db?sslmode=disable"
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Printf("Error connecting to the database: %v", err)
	}
	defer db.Close()
	time.Sleep(time.Second * 5)

	// Подписка на канал 172.19.0.2
	channel := "my_channel"
	sub, err := sc.Subscribe(channel, msgHandler, stan.DurableName("your-durable-name"))
	if err != nil {
		log.Printf("Error subscribing to channel: %v", err)
	}
	defer sub.Close()
	log.Printf("Subscribed to channel")

	// Инициализация кэша
	cache = make(map[string]OrderDB)

	// Загрузка кэша из базы данных
	loadCacheFromDB()
	time.Sleep(time.Second * 5)

	// Добавьте обработчик HTTP-запросов
	http.HandleFunc("/order", getOrderHandler)

	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// Ожидание сигналов завершения
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Ожидание сигналов или завершение по Ctrl+C
	select {
	case sig := <-signalCh:
		log.Printf("Received signal: %v", sig)
		shutdown()
	}

	log.Println("Shutting down...")
}

// Обработчик сообщений NATS Streaming
func msgHandler(msg *stan.Msg) {
	var orderDB OrderDB

	err := json.Unmarshal(msg.Data, &orderDB)
	if err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return
	}

	log.Printf("Decoded order from JSON: %v", orderDB)

	// Заполните поле OrderData данными из msg.Data
	orderDB.OrderData = string(msg.Data)

	// Вставка или обновление данных заказа в базе данных
	if err := insertOrUpdateOrder(orderDB.OrderUID, orderDB.OrderData); err != nil {
		log.Printf("Error inserting or updating order in the database: %v", err)
		return
	}

	// Обновление кэша
	cacheLock.Lock()
	cache[orderDB.OrderUID] = orderDB
	cacheLock.Unlock()

	printCache()

	log.Printf("Loaded order from DB to cache: %v", orderDB)

	// Сохранение данных в базе данных
	_, err = db.Exec("INSERT INTO orders (order_uid, order_data) VALUES ($1, $2) ON CONFLICT (order_uid) DO UPDATE SET order_data = EXCLUDED.order_data", orderDB.OrderUID, orderDB.OrderData)

	if err != nil {
		log.Printf("Error inserting data into the database: %v. Data: %s", err, msg.Data)
		return
	}
}

// Вставка или обновление данных заказа в базе данных
func insertOrUpdateOrder(orderUID, orderData string) error {
	// Проверяем, является ли orderData корректным JSON
	var orderDataJSON map[string]interface{}
	if err := json.Unmarshal([]byte(orderData), &orderDataJSON); err != nil {
		return err
	}

	// Если JSON корректен, выполняем запрос в базу данных
	_, err := db.Exec("INSERT INTO orders (order_uid, order_data) VALUES ($1, $2::jsonb) ON CONFLICT (order_uid) DO UPDATE SET order_data = EXCLUDED.order_data::jsonb", orderUID, orderData)
	return err
}

// Завершение работы, чистка и закрытие ресурсов
func shutdown() {
	log.Println("Shutting down...")
	sc.Close()
	db.Close()
	os.Exit(0)
}

// Загрузка кэша из базы данных
func loadCacheFromDB() {
	rows, err := db.Query("SELECT order_uid, order_data FROM orders")
	if err != nil {
		log.Printf("Error querying data from the database: %v", err)
		return
	}
	defer rows.Close()

	cacheLock.Lock()
	defer cacheLock.Unlock()

	for rows.Next() {
		var orderUID string
		var orderDataStr string
		if err := rows.Scan(&orderUID, &orderDataStr); err != nil {
			log.Printf("Error scanning data from the database: %v", err)
			continue
		}

		log.Printf("Scanning data from the database. OrderUID: %s, OrderData: %s", orderUID, orderDataStr)

		var orderDB OrderDB
		orderDB.OrderUID = orderUID
		orderDB.OrderData = orderDataStr

		cache[orderDB.OrderUID] = orderDB
		log.Printf("Loaded order from DB to cache: %+v", orderDB)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over rows: %v", err)
	}
}

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	// Получение значения параметра order_uid из URL
	orderUID := r.URL.Query().Get("order_uid")

	log.Printf("Received HTTP request for order_uid: %s", orderUID)

	// Поиск заказа в кэше
	cacheLock.RLock()
	orderDB, ok := cache[orderUID]
	cacheLock.RUnlock()

	if !ok {
		// Если заказ не найден, отправить ответ с кодом 404
		log.Printf("Order not found for order_uid: %s", orderUID)
		http.NotFound(w, r)
		return
	}

	printCache()

	// Преобразование данных заказа в JSON
	orderJSON, err := json.Marshal(orderDB)
	if err != nil {
		log.Printf("Error encoding JSON: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Отправка данных в ответе
	log.Printf("Sending JSON response for order_uid: %s", orderUID)
	w.Header().Set("Content-Type", "application/json")
	w.Write(orderJSON)
}
func printCache() {
	cacheLock.RLock()
	defer cacheLock.RUnlock()

	log.Println("Cache Contents:")
	for key, value := range cache {
		log.Printf("Key: %s, Value: %+v", key, value)
	}
}
