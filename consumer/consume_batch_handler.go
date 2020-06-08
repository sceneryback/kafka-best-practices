package consumer

import (
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

// ----- batch handler

type BatchConsumerConfig struct {
	BufferCapacity        int // msg capacity
	MaxBufSize            int // max message size
	TickerIntervalSeconds int
	Callback              func([]*ConsumerSessionMessage) error
}

type batchConsumerGroupHandler struct {
	cfg *BatchConsumerConfig

	ready chan bool

	// buffer
	ticker    *time.Ticker
	msgBuf []*ConsumerSessionMessage

	mu sync.RWMutex

	cb func([]*ConsumerSessionMessage) error
}

func NewBatchConsumerGroupHandler(cfg *BatchConsumerConfig) ConsumerGroupHandler {
	handler := batchConsumerGroupHandler{
		ready: make(chan bool, 0),
		cb:   cfg.Callback,
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
func (h *batchConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *batchConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchConsumerGroupHandler) WaitReady() {
	<-h.ready
	return
}

func (h *batchConsumerGroupHandler) Reset() {
	h.ready = make(chan bool, 0)
	return
}

func (h *batchConsumerGroupHandler) flushBuffer() {
	if len(h.msgBuf) > 0 {
			if err := h.cb(h.msgBuf); err == nil {
				h.msgBuf = make([]*ConsumerSessionMessage, 0, h.cfg.BufferCapacity)
			}
	}
}

func (h *batchConsumerGroupHandler) insertMessage(msg *ConsumerSessionMessage) {
	h.msgBuf = append(h.msgBuf, msg)
	if len(h.msgBuf) >= h.cfg.MaxBufSize {
		h.flushBuffer()
	}
}

func (h *batchConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

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

