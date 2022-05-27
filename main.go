package main

import (
	"github.com/Ycallaer/golang_kafka/data_generator"
	"github.com/Ycallaer/golang_kafka/kafka"
)

func main() {
	customerData := datagenerator.GenerateDummyData()
	p := kafka.NewProducer("localhost:9092", "specialtopic2", "http://localhost:8081")
	//kafka.StartProducer(p,customerData)
	kafka.StartProducerWithSchemaRegistry(p, customerData)
}
