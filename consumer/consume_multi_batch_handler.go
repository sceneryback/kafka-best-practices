package consumer

import (
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

// ----- batch handler

type MultiBatchConsumerConfig struct {
	BufferCapacity        int // msg capacity
	MaxBufSize            int // max message size
	TickerIntervalSeconds int

	BufChan chan batchMessages
}

type batchMessages []*ConsumerSessionMessage

type multiBatchConsumerGroupHandler struct {
	cfg *MultiBatchConsumerConfig

	ready chan bool

	// buffer
	ticker    *time.Ticker
	msgBuf batchMessages

	mu sync.RWMutex
}

func NewMultiBatchConsumerGroupHandler(cfg *MultiBatchConsumerConfig) ConsumerGroupHandler {
	handler := multiBatchConsumerGroupHandler{
		ready: make(chan bool, 0),
	}

	if cfg.BufferCapacity == 0 {
		cfg.BufferCapacity = 10000
	}
	handler.msgBuf = make([]*ConsumerSessionMessage, 0, cfg.BufferCapacity)
	if cfg.MaxBufSize == 0 {
		cfg.MaxBufSize = 8000
	}

	if cfg.TickerIntervalSeconds == 0 {
		cfg.TickerIntervalSeconds = 60
	}
	handler.cfg = cfg

	handler.ticker = time.NewTicker(time.Duration(cfg.TickerIntervalSeconds) * time.Second)

	return &handler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *multiBatchConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *multiBatchConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *multiBatchConsumerGroupHandler) WaitReady() {
	<-h.ready
	return
}

func (h *multiBatchConsumerGroupHandler) Reset() {
	h.ready = make(chan bool, 0)
	return
}

func (h *multiBatchConsumerGroupHandler) flushBuffer() {
	if len(h.msgBuf) > 0 {
		h.cfg.BufChan <- h.msgBuf
	}
}

func (h *multiBatchConsumerGroupHandler) insertMessage(msg *ConsumerSessionMessage) {
	h.msgBuf = append(h.msgBuf, msg)
	if len(h.msgBuf) >= h.cfg.MaxBufSize {
		h.flushBuffer()
	}
}

func (h *multiBatchConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case message, ok := <-claimMsgChan:
			if ok {
				h.insertMessage(&ConsumerSessionMessage{
					Message: message,
					Session: session,
				})
			} else {
				return nil
			}
		case <-h.ticker.C:
			h.mu.Lock()
			h.flushBuffer()
			h.mu.Unlock()
		}
	}

	return nil
}

