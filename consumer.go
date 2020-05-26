package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
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

func getMessage(data []byte) error {
	var msg Message
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
			fmt.Printf("consumed %d messages at speed %.2f/s", count, float64(count) / time.Since(start).Seconds())
		}
		return nil
	})
	consumer, err := NewConsumerGroup([]string{topic}, "sync-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartBatchConsumer(topic string) (ConsumerGroup, error) {
	var count int64
	// 1. sync consumer
	handler, err := NewSyncConsumerGroupHandler(func(bytes []byte) error {

	})
	if err != nil {
		return nil, err
	}
	consumer, err := NewConsumerGroup([]string{topic}, "sync-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartMultiBatchConsumer(topic string) (ConsumerGroup, error) {
	var count int64
	// 1. sync consumer
	handler, err := NewSyncConsumerGroupHandler(func(bytes []byte) error {

	})
	if err != nil {
		return nil, err
	}
	consumer, err := NewConsumerGroup([]string{topic}, "sync-consumer", handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
