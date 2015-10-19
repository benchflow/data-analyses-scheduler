package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
)

var c Configuration
var kafkaIp string
var kafkaPort string
var sparkSubmitOptions string

type Configuration struct {
	Settings []struct {
		Topic string `json:"topic"`
		Scripts []string `json:"scripts"`
		} `json:settings`
	}

func consumeFromTopic(name string) {
	go func() {
		config := sarama.NewConfig()
		consumer, err := sarama.NewConsumer([]string{kafkaIp+":"+kafkaPort}, config)
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
			for _, t := range c.Settings {
				if t.Topic == name {
					for _, s := range t.Scripts {
						fmt.Println(name+" topic, submitting script "+string(s))
						}
					}
				} 
			// TODO: Sending the task to Spark
			}
		}()
}

func main() {
	
	kafkaIp = "192.168.99.100"
	kafkaPort = "9092"
	sparkSubmitOptions = ""
	
	dat, err := ioutil.ReadFile("config.json")
    if err != nil {
			panic(err)
			}
	err = json.Unmarshal(dat, &c)
	if err != nil {
			panic(err)
			}
	
	for _, sett := range c.Settings {
		consumeFromTopic(sett.Topic)
		}
	
	for true{}
	
	}

