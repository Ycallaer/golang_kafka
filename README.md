# Golang Kafka Project
The following project is a learning project combining Go and Kafka.
We will produce and consume data from kafka, in 1 instance we just push data as is, in the other case we use a JSON schema to validate.

## Requirements
* Go 1.18
* Confluent Kafka Cluster

The following packages have also been installed
```
go get -u github.com/bxcodec/faker/v3
go get -u github.com/riferrei/srclient
go get -u github.com/invopop/jsonschema
```

## Running kafka
The easiest way is to use the [kafka_proto_py](https://github.com/Ycallaer/kafka_proto_py) project.
In this project you can find a `docker-compose` file which spins up kafka and some related tech.

## Running this program
You can run this program in 2 ways. If you want to just produce and consume data start the program as follows:
`go run main.go`
If you want to run the JSON producer and consumer, you can achieve this in the following manner:
`go run main.go json`
