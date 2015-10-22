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

type Configuration struct {
	Settings []struct {
		Topic string `json:"topic"`
		Scripts []struct {
			Script string `json:"script"`
			Dependencies []string `json:"dependencies"`
			} `json:"scripts"`
		} `json:settings`
	}

type SparkSubmit struct {
	Master string
	Dependencies []string
	Script string
	}

func constructSubmitCommand(ss SparkSubmit) exec.Cmd {
	//cmd := exec.Command("./spark-submit", "--deploy-mode", "cluster", "--master", ss.Master, ss.Script)
	cmd := exec.Command("/Users/Gabo/Downloads/spark-1.5.1-bin-hadoop2.6/bin/spark-submit", "--master", ss.Master, "--packages", "TargetHolding:pyspark-cassandra:0.1.5", ss.Script, "local[*]", "localhost", "/Users/Gabo/PycharmProjects/Test/Camunda_dump_example_csv.csv.gz")
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
						ss.Master = sparkMaster
						ss.Dependencies = s.Dependencies
						ss.Script = s.Script
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
	sparkMaster = "spark"
	
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

