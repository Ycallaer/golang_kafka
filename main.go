package main

import (
	"fmt"

	"github.com/Ycallaer/golang_kafka/data_generator"
	"github.com/Ycallaer/golang_kafka/kafka"
)

func main(){
	customerData := datagenerator.GenerateDummyData()
	p := kafka.NewProducer("localhost:9092", "specialtopic2")
	fmt.Print(p)
	kafka.StartProducer(p,customerData)
}