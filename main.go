package main

import (
	"github.com/Ycallaer/golang_kafka/kafka"
)

func main(){
	p := kafka.NewProducer("localhost", 9092, "specialtopic")
	kafka.StartProducer(p)
}