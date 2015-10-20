package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"os/exec"
	"strings"
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
	depString := ""
	for _, d := range ss.Dependencies {
		depString = depString + d + ","
		}
	strings.TrimSuffix(depString, ",")
	cmd := exec.Command("./spark-submit", "--deploy-mode", "cluster", "--master", ss.Master, "--py-files", depString, ss.Script)
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

