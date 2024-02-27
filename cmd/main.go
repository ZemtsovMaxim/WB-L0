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
	sub       stan.Subscription
)

var (
	clusterID = "test-cluster"
	clientID  = "client-id"
	NatsURL   = "nats://nats-streaming:4222"
	pgURL     = "postgres://My_user:1234554321@172.19.0.2:5432/My_db?sslmode=disable"
	channel   = "my_channel"
)

func main() {

	connectToDB(pgURL)
	time.Sleep(time.Second * 5)

	connectToNuts(NatsURL)

	cache = make(map[string]OrderDB)

	loadCacheFromDB()
	time.Sleep(time.Second * 5)

	subscribeOnChannel(channel)

	processingHTTP()

	shutDown()
}
func processingHTTP() {
	http.HandleFunc("/order", getOrderHandler)

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Ошибка HTTP-сервера: %v", err)
		}
	}()
}

func shutDown() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signalCh:
		log.Printf("Received signal: %v", sig)
		closeAll()
	}

	log.Println("Shutting down...")
}

func subscribeOnChannel(channel string) (stan.Subscription, error) {
	var err error
	sub, err = sc.Subscribe(channel, msgHandler, stan.DurableName("my-application"))
	if err != nil {
		log.Printf("Error subscribing to channel: %v", err)
		return nil, err
	}
	log.Printf("Subscribed to channel")
	return sub, nil
}

func connectToDB(pgURL string) (*sql.DB, error) {
	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Printf("Error connecting to the database: %v", err)
		return nil, err
	}
	log.Printf("Connected to DB")
	return db, nil
}

func connectToNuts(NatsURL string) (stan.Conn, error) {
	var err error
	sc, err = stan.Connect(clusterID, clientID, stan.NatsURL(NatsURL))
	if err != nil {
		log.Printf("Error connecting to NATS Streaming: %v", err)
		return nil, err
	}
	return sc, nil
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

	orderDB.OrderData = string(msg.Data)

	if err := insertOrUpdateOrder(orderDB.OrderUID, orderDB.OrderData); err != nil {
		log.Printf("Error inserting or updating order in the database: %v", err)
		return
	}

	cacheLock.Lock()
	cache[orderDB.OrderUID] = orderDB
	cacheLock.Unlock()

	_, err = db.Exec("INSERT INTO orders (order_uid, order_data) VALUES ($1, $2) ON CONFLICT (order_uid) DO UPDATE SET order_data = EXCLUDED.order_data", orderDB.OrderUID, orderDB.OrderData)

	if err != nil {
		log.Printf("Error inserting data into the database: %v. Data: %s", err, msg.Data)
		return
	}
}

func insertOrUpdateOrder(orderUID, orderData string) error {
	// Валидация
	var orderDataJSON map[string]interface{}
	if err := json.Unmarshal([]byte(orderData), &orderDataJSON); err != nil {
		return err
	}
	_, err := db.Exec("INSERT INTO orders (order_uid, order_data) VALUES ($1, $2::jsonb) ON CONFLICT (order_uid) DO UPDATE SET order_data = EXCLUDED.order_data::jsonb", orderUID, orderData)
	return err
}

func closeAll() {
	log.Println("Shutting down...")
	db.Close()
	sc.Close()
	sub.Close()
	os.Exit(0)
}

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
	orderUID := r.URL.Query().Get("order_uid")

	log.Printf("Received HTTP request for order_uid: %s", orderUID)

	// Поиск заказа в кэше
	cacheLock.RLock()
	orderDB, ok := cache[orderUID]
	cacheLock.RUnlock()

	if !ok {
		log.Printf("Order not found for order_uid: %s", orderUID)
		http.NotFound(w, r)
		return
	}

	printCache()

	orderJSON, err := json.Marshal(orderDB)
	if err != nil {
		log.Printf("Error encoding JSON: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

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
