package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Ycallaer/golang_kafka/data_generator"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaProducer struct{
	boostrapServer string
	topic string
}

func NewProducer(bootstrap_server string, topic string ) *kafkaProducer{

	return &kafkaProducer{boostrapServer: bootstrap_server, topic:topic}
}

func StartProducer(p *kafkaProducer, dataSet []datagenerator.Customer){
	fmt.Println("Initialising the producer")
	producer , err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": p.boostrapServer})

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
	for _, customer := range dataSet{
		//fmt.Println(customer)
		v , _ := json.Marshal(customer)
		fmt.Println(string(v))
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: v,
		}, nil)
	}
	// for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
	// 	producer.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(word),
	// 	}, nil)
	// }

	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
}