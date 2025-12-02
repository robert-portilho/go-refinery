package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync" // Importante para esperar as goroutines
	"time"

	"github.com/segmentio/kafka-go"
)

type User struct {
	Email string `json:"email"`
}

type Order struct {
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"total_amount"`
	User       User    `json:"usuario"`
	Timestamp  int64   `json:"timestamp"`
}

// sendOrder é a função que será executada por cada goroutine
func sendOrder(w *kafka.Writer, order Order, wg *sync.WaitGroup) {
	// Decrementa o contador do WaitGroup ao final da função
	defer wg.Done()

	bytes, err := json.Marshal(order)
	if err != nil {
		log.Printf("Failed to marshal order: %v", err)
		return
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(order.CustomerID),
			Value: bytes,
		},
	)
	if err != nil {
		// O Writer do kafka-go é thread-safe, mas pode haver erros de rede/conexão
		log.Printf("Failed to write message for customer %s: %v", order.CustomerID, err)
	}
}

func main() {
	// Inicialização e setup permanecem os mesmos
	brokers := flag.String("brokers", "localhost:29092", "Kafka brokers")
	topic := flag.String("topic", "orders", "Kafka topic")
	count := flag.Int("count", 1000, "Number of messages to send")
	flag.Parse()

	// O kafka.Writer é thread-safe e pode ser usado por múltiplas goroutines.
	w := &kafka.Writer{
		Addr:     kafka.TCP(*brokers),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
		// Recomenda-se ajustar o BatchSize ou o Linger:
		// BatchSize: Número máximo de mensagens a serem agrupadas em um único lote.
		// Linger: Tempo máximo para esperar por mais mensagens antes de enviar um lote.
		BatchSize: 10000,
		// Linger:  time.Millisecond * 10,
	}
	defer w.Close()

	log.Printf("Sending %d messages to %s using goroutines...", *count, *topic)
	start := time.Now()

	// WaitGroup para esperar que todas as goroutines terminem
	var wg sync.WaitGroup

	for i := 0; i < *count; i++ {
		order := Order{
			CustomerID: fmt.Sprintf("CUST-%d", rand.Intn(1000)),
			Amount:     rand.Float64() * 200.0, // 0 to 200
			User: User{
				Email: fmt.Sprintf("user%d@example.com", rand.Intn(1000)),
			},
			Timestamp: time.Now().Unix(),
		}

		// Adiciona 1 ao contador do WaitGroup
		wg.Add(1)

		// Inicia uma nova goroutine para enviar a ordem
		go sendOrder(w, order, &wg)
	}

	// Espera que o contador do WaitGroup chegue a zero
	wg.Wait()

	duration := time.Since(start)
	log.Printf("Sent %d messages in %v (%.2f msg/s) concurrently.", *count, duration, float64(*count)/duration.Seconds())
}
