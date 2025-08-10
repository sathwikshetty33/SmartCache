package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

var (
	rdb         *redis.Client
	kafkaWriter *kafka.Writer
	ctx         = context.Background()
)

type InitRequest struct {
	UserID        string `json:"user_id"`
	RedisHost     string `json:"redis_host"`
	RedisPort     int    `json:"redis_port"`
	RedisPassword string `json:"redis_password"`
}

type LogRequest struct {
	UserID     string `json:"user_id"`
	ResourceID string `json:"resource_id"`
	Action     string `json:"action"`
	Hit        bool   `json:"hit"`
	Timestamp  string `json:"timestamp"`
}

func initHandler(w http.ResponseWriter, r *http.Request) {
	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	key := "user:" + req.UserID
	rdb.HSet(ctx, key, "host", req.RedisHost, "port", req.RedisPort, "password", req.RedisPassword)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "✅ Redis credentials stored for user: %s\n", req.UserID)
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	var req LogRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "JSON marshal error", http.StatusInternalServerError)
		return
	}

	err = kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(req.UserID),
		Value: data,
	})
	if err != nil {
		log.Println("❌ Kafka write error:", err)
		http.Error(w, "Kafka error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "✅ Log pushed to Kafka\n")
}

func main() {
	// Connect to internal Redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("internal-redis:6379"), // e.g., "internal-redis:6379"
		Password: "",                               // No password for internal store
		DB:       0,
	})

	// Kafka Writer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(os.Getenv("KAFKA_BROKER")),
		Topic:    "cache_access_logs",
		Balancer: &kafka.LeastBytes{},
	}

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/log", logHandler)

	log.Println("🚀 API Gateway running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
