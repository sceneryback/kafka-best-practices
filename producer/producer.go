package producer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type Producer struct {
	p     sarama.AsyncProducer
}

func NewProducer(broker string) (*Producer, error) {
	producer, err := sarama.NewAsyncProducer([]string{broker}, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	return &Producer{
		p:     producer,
	}, nil
}

type Message struct {
	Id int `json:"id"`
}

func (p *Producer) StartProduce(done chan struct{}, topic string) {
	start := time.Now()
	for i := 0; ; i++ {
		msg := Message{i}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		select {
		case <-done:
			return
		case p.p.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msgBytes),
		}:
			if i % 5000 == 0 {
				fmt.Printf("produced %d messages with speed %.2f/s\n", i, float64(i) / time.Since(start).Seconds())
			}
		case err := <-p.p.Errors():
			fmt.Printf("Failed to send message to kafka, err: %s, msg: %s\n", err, msgBytes)
		}
	}
}

func (p *Producer) Close() error {
	if p != nil {
		return p.p.Close()
	}
	return nil
}
