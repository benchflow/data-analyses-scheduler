package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"os/exec"
	"os"
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
			Packages string `json:"packages"`
			} `json:"scripts"`
		} `json:settings`
	}

func constructSubmitCommand(ss SparkSubmit) exec.Cmd {
	var args []string
	args = append(args, "--master", ss.SparkMaster)
	args = append(args, "--files", ss.MappingConfiguration)
	args = append(args, "--packages", ss.Packages)
	args = append(args, ss.Script)
	args = append(args, ss.SparkMaster)
	args = append(args, ss.CassandraHost)
	args = append(args, ss.fileLocation)
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	//cmd := exec.Command(sparkHome+"/bin/spark-submit", "--master", sparkMaster, "--files", ss.MappingConfiguration, "--packages", "TargetHolding:pyspark-cassandra:0.1.5", ss.Script, sparkMaster, cassandraHost, ss.fileLocation)
	return *cmd
	}

func kafkaConsumer(name string) sarama.PartitionConsumer {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{kafkaIp+":"+kafkaPort}, config)
	if err != nil {	
		panic(err)
		}
	partConsumer, err := consumer.ConsumePartition(name, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
		}
	return partConsumer
}

func consumeFromTopic(name string) {
	go func() {
		/*
		config := sarama.NewConfig()
		consumer, err := sarama.NewConsumer([]string{kafkaIp+":"+kafkaPort}, config)
		if err != nil {
			panic(err)
			}
		partConsumer, err := consumer.ConsumePartition(name, 0, sarama.OffsetNewest)
		if err != nil {
		   	panic(err)
		   	}
		*/
		consumer := kafkaConsumer(name)
		mc := consumer.Messages()
		fmt.Println("Consuming on topic " + name)
		for true {
			m := <- mc 
			fmt.Println(name+" received: "+string(m.Value))
			for _, t := range c.Settings {
				if t.Topic == name {
					for _, s := range t.Scripts {
						fmt.Println(name+" topic, submitting script "+string(s.Script))
						var ss SparkSubmit
						ss = SparkCommandBuilder.
							Packages(s.Packages).
							Script(s.Script).
							MappingConfiguration(s.MappingConfiguration).
							FileLocation(string(m.Value)).
							CassandraHost(cassandraHost).
							SparkMaster(sparkMaster).
							Build()
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
	/*
	kafkaIp = "192.168.99.100"
	kafkaPort = "9092"
	sparkMaster = "local[*]"
	sparkHome = "/Users/Gabo/Downloads/spark-1.5.1-bin-hadoop2.6"
	cassandraHost = "localhost"
	*/
	kafkaIp = os.Getenv("KAFKA_IP")
	kafkaPort = os.Getenv("KAFKA_PORT")
	sparkMaster = os.Getenv("SPARK_MASTER")
	sparkHome = os.Getenv("SPARK_HOME")
	cassandraHost = os.Getenv("CASSANDRA_IP")
	
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

