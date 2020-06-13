package consumer

import (
	"github.com/Shopify/sarama"
)

type syncConsumerGroupHandler struct {
	ready chan bool

	cb func([]byte) error
}

func NewSyncConsumerGroupHandler(cb func([]byte) error) ConsumerGroupHandler {
	handler := syncConsumerGroupHandler{
		ready: make(chan bool, 0),
		cb:    cb,
	}
	return &handler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *syncConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *syncConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *syncConsumerGroupHandler) WaitReady() {
	<-h.ready
	return
}

func (h *syncConsumerGroupHandler) Reset() {
	h.ready = make(chan bool, 0)
	return
}

func (h *syncConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for message := range claimMsgChan {
		if h.cb(message.Value) == nil {
			session.MarkMessage(message, "")
		}
	}

	return nil
}
