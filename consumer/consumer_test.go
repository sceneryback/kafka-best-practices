package consumer

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"

	"github.com/sceneryback/kafka-best-practices/producer"
)

var (
	testTopicPrefix = "test-topic"
	testBroker      = "127.0.0.1:9092"
)

func testProduce(topic string, limit int) <-chan struct{} {
	var produceDone = make(chan struct{})

	p, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, sarama.NewConfig())
	if err != nil {
		return nil
	}

	go func() {
		defer close(produceDone)

		for i := 0; i < limit; i++ {
			msg := producer.Message{i}
			msgBytes, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			select {
			case p.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(msgBytes),
			}:
			case err := <-p.Errors():
				fmt.Printf("Failed to send message to kafka, err: %s, msg: %s\n", err, msgBytes)
			}
		}
	}()

	return produceDone
}

func TestSyncConsumer(t *testing.T)  {
	limit := 500000

	topic := testTopicPrefix + fmt.Sprintf("%d", time.Now().Unix())

	produceDone := testProduce(topic, limit)

	var consumeMsgMap = make(map[int]struct{})
	var resChan = make(chan int)
	go func() {
		for r := range resChan {
			consumeMsgMap[r] = struct{}{}
		}
	}()

	handler := NewSyncConsumerGroupHandler(func(data []byte) error {
		var msg producer.Message
		err := json.Unmarshal(data, &msg)
		if err != nil {
			return err
		}
		resChan <- msg.Id
		return nil
	})
	consumer, err := NewConsumerGroup(testBroker, []string{topic}, "sync-consumer-" + fmt.Sprintf("%d", time.Now().Unix()), handler)
	if err != nil {
		return
	}
	defer consumer.Close()

	<-produceDone

	time.Sleep(1*time.Second)

	assert.Equal(t, limit, len(consumeMsgMap))
}

func TestBatchConsumer(t *testing.T) {
	limit := 500000

	topic := testTopicPrefix + fmt.Sprintf("%d", time.Now().Unix())

	produceDone := testProduce(topic, limit)

	var consumeMsgMap = make(map[int]struct{})
	var resChan = make(chan int)
	go func() {
		for r := range resChan {
			consumeMsgMap[r] = struct{}{}
		}
	}()

	handler := NewBatchConsumerGroupHandler(&BatchConsumerConfig{
		MaxBufSize: 1000,
		Callback: func(messages []*ConsumerSessionMessage) error {
			for i := range messages {
				var msg producer.Message
				err := json.Unmarshal(messages[i].Message.Value, &msg)
				if err != nil {
					return err
				}
				resChan <- msg.Id
			}
			return nil
		},
	})
	consumer, err := NewConsumerGroup(testBroker, []string{topic}, "batch-consumer-" + fmt.Sprintf("%d", time.Now().Unix()), handler)
	if err != nil {
		return
	}
	defer consumer.Close()

	<-produceDone

	time.Sleep(1*time.Second)

	assert.Equal(t, limit, len(consumeMsgMap))
}

func TestMultiAsyncConsumer(t *testing.T) {
	limit := 500000

	topic := testTopicPrefix + fmt.Sprintf("%d", time.Now().Unix())

	produceDone := testProduce(topic, limit)

	var consumeMsgMap = make(map[int]struct{})
	var resChan = make(chan int)
	go func() {
		for r := range resChan {
			consumeMsgMap[r] = struct{}{}
		}
	}()

	var bufChan = make(chan *ConsumerSessionMessage, 1000)
	for i := 0; i < 8; i++ {
		go func() {
			for message := range bufChan {
				var msg producer.Message
				err := json.Unmarshal(message.Message.Value, &msg)
				if err != nil {
					continue
				}
				resChan <- msg.Id
			}
		}()
	}
	handler := NewMultiAsyncConsumerGroupHandler(&MultiAsyncConsumerConfig{
		BufChan: bufChan,
	})
	consumer, err := NewConsumerGroup(testBroker, []string{topic}, "multi-async-consumer-" + fmt.Sprintf("%d", time.Now().Unix()), handler)
	if err != nil {
		return
	}
	defer consumer.Close()

	<-produceDone

	time.Sleep(1*time.Second)

	assert.Equal(t, limit, len(consumeMsgMap))
}

func TestMultiBatchConsumer(t *testing.T)  {
	limit := 500000

	topic := testTopicPrefix + fmt.Sprintf("%d", time.Now().Unix())

	produceDone := testProduce(topic, limit)

	var consumeMsgMap = make(map[int]struct{})
	var resChan = make(chan int)
	go func() {
		for r := range resChan {
			consumeMsgMap[r] = struct{}{}
		}
	}()

	var bufChan = make(chan batchMessages, 1000)
	for i := 0; i < 8; i++ {
		go func() {
			for messages := range bufChan {
				for j := range messages {
					var msg producer.Message
					err := json.Unmarshal(messages[j].Message.Value, &msg)
					if err != nil {
						continue
					}
					resChan <- msg.Id
				}
			}
		}()
	}
	handler := NewMultiBatchConsumerGroupHandler(&MultiBatchConsumerConfig{
		MaxBufSize: 1000,
		BufChan: bufChan,
		TickerIntervalSeconds: 1,
	})
	consumer, err := NewConsumerGroup(testBroker, []string{topic}, "multi-batch-consumer-" + fmt.Sprintf("%d", time.Now().Unix()), handler)
	if err != nil {
		return
	}
	defer consumer.Close()

	<-produceDone

	time.Sleep(1*time.Second)

	assert.Equal(t, limit, len(consumeMsgMap))
}
