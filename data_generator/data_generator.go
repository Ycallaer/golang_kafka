package datagenerator

import (
	"fmt"
	"github.com/bxcodec/faker/v3"
)



type Customer struct {
	FirstName   string `faker:"first_name"`
	LastName    string `faker:"last_name"`
	Email       string `faker:"email"`
	PhoneNumber string `faker:"phone_number"`
}

type MyCustomers struct {
	Customers []Customer
}

func GenerateDummyData() []Customer {
	c := Customer{}
	customerResult  := MyCustomers{}

	for i := 0; i < 100 ; i++ {
		err := faker.FakeData(&c)
		if err != nil {
			fmt.Println(err)
		}
		customerResult.Customers = append(customerResult.Customers, c)		
	}
	return customerResult.Customers
}
