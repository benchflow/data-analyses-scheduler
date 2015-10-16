package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
)

type Configuration struct {
	Settings []struct {
		Topic string `json:"topic"`
		Scripts []string `json:"scripts"`
		} `json:settings`
	}

func consumeFromTopic(name string) {
	go func() {
		config := sarama.NewConfig()
		consumer, err := sarama.NewConsumer([]string{"192.168.99.100:9092"}, config)
		if err != nil {
			panic(err)
			}
		partConsumer, err := consumer.ConsumePartition(name, 0, sarama.OffsetNewest)
		if err != nil {
		   	panic(err)
		   	}
		mc := partConsumer.Messages()
		fmt.Println("Consuming on topic " + name)
		for true {
			m := <- mc 
			fmt.Println(name+" received: "+string(m.Value))
			// TODO: Sending the task to Spark
			}
		}()
}

func main() {
	
	dat, err := ioutil.ReadFile("config.json")
    if err != nil {
			panic(err)
			}
	var c Configuration
	err = json.Unmarshal(dat, &c)
	if err != nil {
			panic(err)
			}
	
	for _, sett := range c.Settings {
		consumeFromTopic(sett.Topic)
		}
	
	for true{}
	
	}

