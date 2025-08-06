package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type CacheEvent struct {
	ResourceID string    `json:"resource_id"`
	Action     string    `json:"action"`
	Hit        bool      `json:"hit"`
	Timestamp  time.Time `json:"timestamp"`
}

func worker(id int, jobs <-chan kafka.Message, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	insertSQL := `INSERT INTO cache_logs(resource_id, action, hit, timestamp) VALUES ($1, $2, $3, $4)`

	for msg := range jobs {
		var event CacheEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Worker %d ❌ JSON Unmarshal: %v\n", id, err)
			continue
		}

		_, err := db.Exec(insertSQL, event.ResourceID, event.Action, event.Hit, event.Timestamp)
		if err != nil {
			log.Printf("Worker %d ❌ DB Insert: %v\n", id, err)
			continue
		}

		log.Printf("Worker %d ✅ Inserted Resource=%s Action=%s Hit=%v\n",
			id, event.ResourceID, event.Action, event.Hit)
	}
}

func main() {
	// ---------------- Kafka Config ----------------
	kafkaBroker := "localhost:9092" // use localhost:9092 if running outside docker
	topic := "cache_access_logs"
	groupID := "db-updater-group"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	log.Println("✅ Connected to Kafka, waiting for messages...")

	// ---------------- DB Config ----------------
	dbHost := "localhost" // use localhost if running locally
	dbPort := 5432
	dbUser := "smartcache"
	dbPass := "smartcache123"
	dbName := "smartcache"

	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName,
	)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("❌ Failed to connect to DB:", err)
	}
	defer db.Close()

	// ---------------- Worker Pool ----------------
	workerCount := 5
	jobs := make(chan kafka.Message, 100) // buffered channel

	var wg sync.WaitGroup
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, jobs, db, &wg)
	}

	// ---------------- Graceful Shutdown ----------------
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("🛑 Shutting down gracefully...")
		cancel()
		close(jobs)
	}()

	// ---------------- Kafka Consumption Loop ----------------
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("❌ Kafka read error or shutdown:", err)
			break
		}
		jobs <- m
	}

	wg.Wait()
	log.Println("✅ All workers stopped. Exiting...")
}

//  docker run --rm -it   --network=smartcache-net   -v $(pwd):/app   -w /app   golang:1.24.4 go run cmd/main.go
