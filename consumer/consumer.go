package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sceneryback/kafka-best-practices/producer"
	"sync/atomic"
	"time"
)

const (
	SyncMode = "sync"
	BatchMode = "batch"
	MultiBatchMode = "multiBatch"
)

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}

type ConsumerGroup struct {
	cg sarama.ConsumerGroup
}

func NewConsumerGroup(topics []string, group string, handler ConsumerGroupHandler) (*ConsumerGroup, error) {
	ctx := context.Background()
	client, err := sarama.NewConsumerGroup([]string{"127.0.0.1:9092"}, group, sarama.NewConfig())
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					panic(err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()

	handler.WaitReady() // Await till the consumer has been set up

	return &ConsumerGroup{
		cg: client,
	}, nil
}

func (c *ConsumerGroup) Close() error {
	return c.cg.Close()
}

type ConsumerSessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

func getMessage(data []byte) error {
	var msg producer.Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	return nil
}

func StartSyncConsumer(topic string) (*ConsumerGroup, error) {
	var count int64
	var start = time.Now()
	handler := NewSyncConsumerGroupHandler(func(data []byte) error {
		if err := getMessage(data); err != nil {
			return err
		}
		count++
		if count % 5000 == 0 {
			fmt.Printf("sync consumer consumed %d messages at speed %.2f/s\n", count, float64(count) / time.Since(start).Seconds())
		}
		return nil
	})
	consumer, err := NewConsumerGroup([]string{topic}, "sync-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartBatchConsumer(topic string) (*ConsumerGroup, error) {
	var count int64
	var start = time.Now()
	handler := NewBatchConsumerGroupHandler(&BatchConsumerConfig{
		Callback: func(messages []*ConsumerSessionMessage) error {
			for i := range messages {
				if err := getMessage(messages[i].Message.Value); err == nil {
					messages[i].Session.MarkMessage(messages[i].Message, "")
				}
			}
			count += int64(len(messages))
			if count % 5000 == 0 {
				fmt.Printf("batch consumer consumed %d messages at speed %.2f/s\n", count, float64(count) / time.Since(start).Seconds())
			}
			return nil
		},
	})
	consumer, err := NewConsumerGroup([]string{topic}, "batch-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartMultiBatchConsumer(topic string) (*ConsumerGroup, error) {
	var count int64
	var start = time.Now()
	var bufChan = make(chan batchMessages, 5000)
	for i := 0; i < 8; i++ {
		go func() {
			for messages := range bufChan {
				for j := range messages {
					if err := getMessage(messages[j].Message.Value); err == nil {
						messages[j].Session.MarkMessage(messages[j].Message, "")
					}
				}
				cur := atomic.AddInt64(&count, int64(len(messages)))
				if cur % 5000 == 0 {
					fmt.Printf("batch consumer consumed %d messages at speed %.2f/s\n", cur, float64(cur) / time.Since(start).Seconds())
				}
			}
		}()
	}
	handler := NewMultiBatchConsumerGroupHandler(&MultiBatchConsumerConfig{
		BufChan: bufChan,
	})
	consumer, err := NewConsumerGroup([]string{topic}, "multi-batch-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
