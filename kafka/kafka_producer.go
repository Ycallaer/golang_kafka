package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/Ycallaer/golang_kafka/data_generator"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
)

type kafkaProducer struct {
	boostrapServer    string
	topic             string
	schemaRegistryURL string
}

// Creates a KafkaProducer instance
func NewProducer(bootstrap_server string, topic string, schemaRegistryURL string) *kafkaProducer {

	return &kafkaProducer{boostrapServer: bootstrap_server, topic: topic, schemaRegistryURL: schemaRegistryURL}
}

//Starts a regular producer
func StartProducer(p *kafkaProducer, dataSet []datagenerator.Customer) {
	fmt.Println("Initialising the producer")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": p.boostrapServer})

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
	for _, customer := range dataSet {
		v, _ := json.Marshal(customer)
		fmt.Println(string(v))
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          v,
		}, nil)
	}

	producer.Flush(15 * 1000)
}

// Produces a JSON record to kafka using schema registry
func StartJsonProducerWithSchemaRegistry(p *kafkaProducer, dataSet []datagenerator.Customer) {
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(p.schemaRegistryURL)
	schema, err := schemaRegistryClient.GetLatestSchema(p.topic)
	if schema == nil {
		schemaBytes, err := ioutil.ReadFile("/home/yves/PersonalProjects/golang_kafka/Consumer.json")
		if err != nil {
			panic(fmt.Sprintf("File not found %s", err))
		}
		schema, err = schemaRegistryClient.CreateSchema(p.topic, string(schemaBytes), srclient.Json)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": p.boostrapServer})

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

	for _, customer := range dataSet {
		value, _ := json.Marshal(customer)
		var recordValue []byte

		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, value...)

		fmt.Println(string(value))
		key, _ := uuid.NewUUID()
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key.String()), Value: recordValue}, nil)
	}
	producer.Flush(15 * 1000)
}
