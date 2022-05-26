package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaProducer struct{
	boostrapServer string
	port int
	topic string
}

func NewProducer(bootstrap_server string, port int, topic string ) *kafkaProducer{

	return &kafkaProducer{boostrapServer: bootstrap_server, port: port, topic:topic}
}

func StartProducer(p *kafkaProducer){
	fmt.Println("Initialising the producer")
	producer , err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := p.topic
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
}