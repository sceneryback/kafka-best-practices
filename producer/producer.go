package producer

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type Producer struct {
	p     sarama.AsyncProducer
	exitC chan struct{}
}

func NewProducer() (*Producer, error) {
	producer, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	return &Producer{
		p:     producer,
		exitC: make(chan struct{}),
	}, nil
}

type Message struct {
	Id int `json:"id"`
}

func (p *Producer) StartProduce(topic string) {
	start := time.Now()
	for i := 0; ; i++ {
		msg := Message{i}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		select {
		case p.p.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msgBytes),
		}:
			if i % 5000 == 0 {
				fmt.Printf("produced %d messages with speed %.2f/s\n", i, float64(i) / time.Since(start).Seconds())
			}
		case err := <-p.p.Errors():
			fmt.Printf("Failed to send message to kafka, err: %s, msg: %s\n", err, msgBytes)
		case <-p.exitC:
			return
		}
	}
}

func (p *Producer) Close() error {
	defer func() {
		p.exitC <- struct{}{}
	}()
	if p != nil {
		return p.p.Close()
	}
	return nil
}
