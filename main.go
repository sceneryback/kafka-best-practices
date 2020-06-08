package main

import (
	"flag"
	"fmt"
	"github.com/sceneryback/kafka-best-practices/consumer"
	"github.com/sceneryback/kafka-best-practices/producer"
	"os"
	"time"
)

var mode string

func init() {
	flag.StringVar(&mode, "m", "", "consuming mode, 'sync', 'batch' or 'multiBatch'")
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

	var topic = "test-practice-topic-" + mode + fmt.Sprintf("-%d", time.Now().Unix())

	producer, err := producer.NewProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	go producer.StartProduce(topic)

	switch mode {
	case consumer.SyncMode:
		// 1. sync consumer
		consumer, err := consumer.StartSyncConsumer(topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	case consumer.BatchMode:
		// 2. batch consumer
		consumer, err := consumer.StartBatchConsumer(topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	case consumer.MultiBatchMode:
		// 3. multi batch consumer
		consumer, err := consumer.StartMultiBatchConsumer(topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	}

	time.Sleep(30*time.Second)
	//c := make(chan os.Signal, 1)
	//signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	//fmt.Println("received signal", <-c)
}


