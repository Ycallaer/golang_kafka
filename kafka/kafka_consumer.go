package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

type kafkaConsumer struct {
	boostrapServer    string
	topic             string
	schemaRegistryURL string
	group_id		  string
}

//Initializes KafkaConsumer for the given parameters
func NewConsumer(bootstrap_server string, topic string, schemaRegistryURL string, group_id string) *kafkaConsumer {

	return &kafkaConsumer{boostrapServer: bootstrap_server, topic: topic, schemaRegistryURL: schemaRegistryURL, group_id: group_id}
}

//Starts a regular consumer 
func StartConsumer(c *kafkaConsumer){

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": c.boostrapServer,
		"group.id":          c.group_id,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof": true,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	fmt.Sprintf("Consumer subscribing to topic %s", c.topic)
	consumer.SubscribeTopics([]string{c.topic, "^aRegex.*[Tt]opic"}, nil)

	run := true
	fmt.Println("Starting consumer processing")
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}

//Takes a byte array and validates if the JSON schema is correct
func validateJson(kafkaMessage []byte){
	var s interface{}
	json.Unmarshal(kafkaMessage,&s)
	m := s.(map[string]interface{})
	validationErrors := make([]string,0)
	if len(strings.TrimSpace(m["firstname"].(string))) == 0 {
		validationErrors = append(validationErrors, "firstname is missing. Invalid record.")
	}
	if len(strings.TrimSpace(m["lastname"].(string))) == 0 {
		validationErrors = append(validationErrors, "lastname is missing. Invalid record.")
	}

	if len(validationErrors) > 0 {
		for i := 0; i < len(validationErrors) -1 ; i++ {
			fmt.Println("The following validation issue(s) was detected: %s", validationErrors[i])
		}
	}else{
		fmt.Printf("Record result is: firstname: %s, lastname: %s, email: %s, phone: %s", m["firstname"].(string), m["lastname"].(string), m["email"].(string), m["phonenumber"].(string))
		fmt.Println()
	}
}

//Starts a consumer for JSON records , using schema registry
func StartJsonConsumer(c *kafkaConsumer){

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": c.boostrapServer,
		"group.id":          c.group_id,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof": true,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	fmt.Sprintf("Consumer subscribing to topic %s", c.topic)
	consumer.SubscribeTopics([]string{c.topic}, nil)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	
	run := true
	fmt.Println("Starting consumer processing")
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Unassign()
			case *kafka.Message:
				schemaID := binary.BigEndian.Uint32(e.Value[1:5])
				schema, err := schemaRegistryClient.GetSchema(int(schemaID))
				if err != nil {
					panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaID, err))
				}
				res := schema.JsonSchema().String()
				fmt.Print(res)
				validateJson(e.Value[5:])
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}