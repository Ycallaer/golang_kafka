package main

import (
	"github.com/Ycallaer/golang_kafka/data_generator"
	"github.com/Ycallaer/golang_kafka/kafka"
	"os"
)

func main() {
	customerData := datagenerator.GenerateDummyData()
	var arg string
	if len(os.Args[1:]) > 0 {
		arg = os.Args[1]
	}else {
		arg = "basic"
	}
	
	p := kafka.NewProducer("localhost:9092", "specialtopic6", "http://localhost:8081")
	if arg == "json"{
		kafka.StartJsonProducerWithSchemaRegistry(p, customerData)
	}else{
		kafka.StartProducer(p,customerData)
	}
	c := kafka.NewConsumer("localhost:9092","specialtopic6", "http://localhost:8081","awesomegroupid")
	if arg == "json"{
		kafka.StartJsonConsumer(c)
	}else{
		kafka.StartConsumer(c)
	}
}
