package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Orders struct {
	Order_uid          string    `json:"order_uid"`
	Track_number       string    `json:"track_number"`
	Entry              string    `json:"entry"`
	Delivery           Delivery  `json:"delivery"`
	Payment            Payment   `json:"payment"`
	Items              []Item    `json:"items"`
	Locale             string    `json:"locale"`
	Internal_signature string    `json:"internal_signature"`
	Customer_id        string    `json:"customer_id"`
	Delivery_service   string    `json:"delivery_service"`
	Shardkey           string    `json:"shardkey"`
	Sm_id              int       `json:"sm_id"`
	Date_created       time.Time `json:"date_created"`
	Oof_shard          string    `json:"oof_shard"`
}

type Delivery struct {
	Delivery_id int
	Name        string `json:"name"`
	Phone       string `json:"phone"`
	Zip         string `json:"zip"`
	City        string `json:"city"`
	Address     string `json:"address"`
	Region      string `json:"region"`
	Email       string `json:"email"`
}

type Payment struct {
	Payment_id    int
	Transaction   string `json:"transaction"`
	Request_id    string `json:"request_id"`
	Currency      string `json:"currency"`
	Provider      string `json:"provider"`
	Amount        int    `json:"amount"`
	Payment_dt    int    `json:"payment_dt"`
	Bank          string `json:"bank"`
	Delivery_cost int    `json:"delivery_cost"`
	Goods_total   int    `json:"goods_total"`
	Custom_fee    int    `json:"custom_fee"`
}

type Item struct {
	Chrt_id      int    `json:"chrt_id"`
	Track_number string `json:"track_number"`
	Price        int    `json:"price"`
	Rid          string `json:"rid"`
	Name         string `json:"name"`
	Sale         int    `json:"sale"`
	Size         string `json:"size"`
	Total_price  int    `json:"total_price"`
	Nm_id        int    `json:"nm_id"`
	Brand        string `json:"brand"`
	Status       int    `json:"status"`
}

var cache = make(map[string]Orders)

func createDelivery(db *sql.Tx, name, phone, zip, city, address, region, email string) (int, error) {
	var deliveryID int
	err := db.QueryRow(`
		INSERT INTO delivery (name, phone, zip, city, address, region, email)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING delivery_id
	`, name, phone, zip, city, address, region, email).Scan(&deliveryID)

	if err != nil {
		return 0, err
	}

	return deliveryID, nil
}

func createPayment(db *sql.Tx, transaction string, request_id string, currency string,
	provider string, amount int, payment_dt int, bank string, delivery_cost int,
	goods_total int, custom_fee int) (int, error) {

	var paymentID int
	err := db.QueryRow(`
		INSERT INTO payment (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING payment_id
	`, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee).Scan(&paymentID)

	if err != nil {
		return 0, err
	}
	return paymentID, nil
}

func createItems(db *sql.Tx, items []Item, orderUID string) error {
	var itemID int

	for _, item := range items {
		err := db.QueryRow(`
		INSERT INTO items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING items_id
	`, item.Chrt_id, item.Track_number, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.Total_price, item.Nm_id, item.Brand, item.Status).Scan(&itemID)
		if err != nil {
			return err
		}
		_, err = db.Exec(`
			INSERT INTO orders_items (order_uid, item_id)
			VALUES ($1, $2)
		`, orderUID, itemID)
		if err != nil {
			return err
		}

	}

	return nil
}

func createOrders(db *sql.DB, order_name []byte) error {

	var order Orders
	if err := json.Unmarshal(order_name, &order); err != nil {
		return fmt.Errorf("error parsing JSON: %v", err)
	}
	cache[order.Order_uid] = order
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	var deliveryID int
	deliveryID, err = createDelivery(tx, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return fmt.Errorf("error inserting delivery %v", err)
	}

	var paymentID int
	paymentID, err = createPayment(tx, order.Payment.Transaction, order.Payment.Request_id, order.Payment.Currency, order.Payment.Provider, order.Payment.Amount, order.Payment.Payment_dt, order.Payment.Bank, order.Payment.Delivery_cost, order.Payment.Goods_total, order.Payment.Custom_fee)
	if err != nil {
		return fmt.Errorf("error inserting payment %v", err)
	}

	err = createItems(tx, order.Items, order.Order_uid)
	if err != nil {
		return fmt.Errorf("error inserting items: %v", err)
	}
	_, err = tx.Exec(`
		INSERT INTO orders (order_uid, track_number, entry, delivery_id, payment_id, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING order_uid
	`, order.Order_uid, order.Track_number, order.Entry, deliveryID, paymentID,
		order.Locale, order.Internal_signature, order.Customer_id, order.Delivery_service,
		order.Shardkey, order.Sm_id, order.Date_created, order.Oof_shard)

	if err != nil {
		return fmt.Errorf("error inserting orders: %v", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}
	return nil
}

func ReadSQLFile(filename string) (string, error) {
	absPath, err := filepath.Abs(filename)
	if err != nil {
		return "", fmt.Errorf("error getting absolute path: %v", err)
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("error reading SQL file: %v", err)
	}

	return string(content), nil
}
func FillCache() error {
	db, err := InitDB()
	if err != nil {
		log.Printf("Failed to initialize database: %v", err)
		return fmt.Errorf("failed to initialize database: %v", err)
	}
	defer db.Close()
	log.Println("Connected to database successfully")
	orderUids, err := db.Query("SELECT order_uid FROM orders")
	if err != nil {
		return fmt.Errorf("error while receiving data: %v", err)
	}
	for orderUids.Next() {
		var orderUid string
		if err := orderUids.Scan(&orderUid); err != nil {
			return fmt.Errorf("error while get order_uid: %v", err)
		}
		order, err := GetOrderByUID(orderUid)
		if err != nil {
			return fmt.Errorf("error while retrieving order data: %v", err)
		}
		cache[orderUid] = order
	}
	log.Println("Cache filled successfully")
	return nil
}
func GetOrderByUID(orderUID string) (Orders, error) {

	value, ok := cache[orderUID]
	if ok {
		log.Print("Value was in cache")
		return value, nil
	}

	db, err := InitDB()
	if err != nil {
		log.Printf("Failed to initialize database: %v", err)
		return Orders{}, fmt.Errorf("failed to initialize database: %v", err)
	}
	defer db.Close()

	query, err := ReadSQLFile("get_query.sql")
	if err != nil {
		return Orders{}, fmt.Errorf("error reading SQL file: %v", err)
	}

	var order Orders
	var itemsJSON []byte

	err = db.QueryRow(query, orderUID).Scan(
		&order.Order_uid,
		&order.Track_number,
		&order.Entry,
		&order.Locale,
		&order.Internal_signature,
		&order.Customer_id,
		&order.Delivery_service,
		&order.Shardkey,
		&order.Sm_id,
		&order.Date_created,
		&order.Oof_shard,
		&order.Delivery.Name,
		&order.Delivery.Phone,
		&order.Delivery.Zip,
		&order.Delivery.City,
		&order.Delivery.Address,
		&order.Delivery.Region,
		&order.Delivery.Email,
		&order.Payment.Transaction,
		&order.Payment.Request_id,
		&order.Payment.Currency,
		&order.Payment.Provider,
		&order.Payment.Amount,
		&order.Payment.Payment_dt,
		&order.Payment.Bank,
		&order.Payment.Delivery_cost,
		&order.Payment.Goods_total,
		&order.Payment.Custom_fee,
		&itemsJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return Orders{}, sql.ErrNoRows
		}
		return Orders{}, fmt.Errorf("error executing query: %v", err)
	}

	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return Orders{}, fmt.Errorf("error parsing items JSON: %v", err)
	}
	cache[order.Order_uid] = order
	return order, nil
}

func WriteOrderToFile(order Orders, filename string) error {
	data, err := json.MarshalIndent(order, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %v", err)
	}

	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	return nil
}

func InitDB() (*sql.DB, error) {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "myuser")
	password := getEnv("DB_PASSWORD", "mypassword")
	dbname := getEnv("DB_NAME", "mydatabase")
	sslmode := getEnv("DB_SSLMODE", "disable")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)

	var db *sql.DB
	var err error

	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			log.Printf("Failed to open database: %v. Retrying...", err)
			time.Sleep(3 * time.Second)
			continue
		}

		err = db.Ping()
		if err == nil {
			break
		}

		log.Printf("Failed to connect to database: %v. Retrying...", err)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %v", err)
	}

	log.Println("Connected to database successfully")
	return db, nil
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		tmpl := `
        <!DOCTYPE html>
        <html>
        <head>
            <title>Поиск заказа</title>
        </head>
        <body>
            <form onsubmit="redirectToOrder(); return false;">
                <input type="text" id="order_uid" placeholder="Введите ID заказа" required>
                <button type="submit">Отправить</button>
            </form>
            <script>
                function redirectToOrder() {
                    var orderId = document.getElementById('order_uid').value;
                    window.location.href = '/order/' + encodeURIComponent(orderId);
                }
            </script>
        </body>
        </html>`
		fmt.Fprint(w, tmpl)
	}
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		pathParts := strings.Split(r.URL.Path, "/")
		if len(pathParts) < 3 {
			http.Error(w, "Invalid URL format", http.StatusBadRequest)
			return
		}

		orderUID := pathParts[2]

		order, err := GetOrderByUID(orderUID)
		if err == sql.ErrNoRows {
			http.Error(w, "Заказ не найден!", http.StatusNotFound)
			return
		} else if err != nil {
			log.Printf("Error getting order: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		orderJSON, _ := json.MarshalIndent(order, "", "  ")
		w.Write(orderJSON)
	}
}

func main() {
	time.Sleep(3 * time.Second)
	err := FillCache()
	if err != nil {
		log.Printf("Error filling cache: %v", err)
	}
	db, err := InitDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		http.HandleFunc("/", homeHandler)
		http.HandleFunc("/order/", orderHandler)
		log.Println("Starting HTTP server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
			cancel()
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		Topic:          "orders",
		GroupID:        "my_group",
		CommitInterval: 0,
	})

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Возникла ошибка: %v", err)
		}
		err = createOrders(db, msg.Value)
		if err != nil {
			log.Printf("Возникла ошибка: %v", err)
		}

		err = reader.CommitMessages(context.Background(), msg)
		if err != nil {
			log.Print(err)
		}
	}
}
