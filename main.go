package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var mode string

func init() {
	flag.StringVar(&mode, "m", "", "consuming mode")
}

func main()  {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	if mode == "" {
		flag.Usage()
		return
	}

	var topic = "test-practice-topic-" + mode

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	producer, err := NewProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	go producer.StartProduce(topic)

	switch mode {
	case SyncMode:
		// 1. sync consumer
		consumer, err := StartSyncConsumer(topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()

	}

	fmt.Println("received signal", <-c)
}


