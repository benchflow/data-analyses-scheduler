package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"os/exec"
)

var c Configuration
var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string

type Configuration struct {
	Settings []struct {
		Topic string `json:"topic"`
		Scripts []struct {
			Script string `json:"script"`
			MappingConfiguration string `json:"mappingConfiguration"`
			Dependencies []string `json:"dependencies"`
			} `json:"scripts"`
		} `json:settings`
	}

type SparkSubmit struct {
	Dependencies []string
	Script string
	MappingConfiguration string
	fileLocation string
	}

func constructSubmitCommand(ss SparkSubmit) exec.Cmd {
	cmd := exec.Command(sparkHome+"/bin/spark-submit", "--master", sparkMaster, "--packages", "TargetHolding:pyspark-cassandra:0.1.5", ss.Script, sparkMaster, cassandraHost, ss.fileLocation)
	return *cmd
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
						fmt.Println(name+" topic, submitting script "+string(s.Script)+", dependencies: "+string(s.Dependencies[0]))
						var ss SparkSubmit
						ss.Dependencies = s.Dependencies
						ss.Script = s.Script
						ss.MappingConfiguration = s.MappingConfiguration
						ss.fileLocation = string(m.Value)
						cmd := constructSubmitCommand(ss)
						err := cmd.Start()
						cmd.Wait()
						if err != nil {
							panic(err)
							}
						}
						fmt.Println("Script sent")
					}
				} 
			}
		}()
}

func main() {
	
	kafkaIp = "192.168.99.100"
	kafkaPort = "9092"
	sparkMaster = "local[*]"
	sparkHome = "/Users/Gabo/Downloads/spark-1.5.1-bin-hadoop2.6"
	cassandraHost = "localhost"
	
	dat, err := ioutil.ReadFile("config/config.json")
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

