package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"os/exec"
	//"os"
	"sync"
	"strings"
)

var requirementChannels map[string] chan bool

var c Configuration
var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string
var minioHost string
var waitGroup sync.WaitGroup

type Configuration struct {
	TransformersSettings []TransformerSetting `json:"transformers_settings"`
	AnalysersSettings []AnalyserSetting `json:"analysers_settings"`
	}

type AnalyserSetting struct {
	Requirements string `json:"requirements"`
	Scripts []Script `json:"scripts"`
}

type TransformerSetting struct {
	Topic string `json:"topic"`
	Scripts []Script `json:"scripts"`
}

type Script struct {
	Script string `json:"script"`
	Files string `json:"files"`
	PyFiles string `json:"py_files"`
	Packages string `json:"packages"`
	}

func constructTransformerSubmitCommand(ss SparkSubmit) exec.Cmd {
	var args []string
	args = append(args, "--master", ss.SparkMaster)
	args = append(args, "--files", ss.Files)
	args = append(args, "--py-files", ss.PyFiles)
	args = append(args, "--packages", ss.Packages)
	args = append(args, ss.Script)
	args = append(args, ss.SparkMaster)
	args = append(args, ss.CassandraHost)
	args = append(args, ss.MinioHost)
	args = append(args, ss.FileLocation)
	args = append(args, ss.TrialID)
	args = append(args, ss.ContainerID)
	fmt.Println(args)
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	return *cmd
	}

func constructAnalyserSubmitCommand(ss SparkSubmit) exec.Cmd {
	var args []string
	args = append(args, "--master", ss.SparkMaster)
	args = append(args, "--files", ss.Files)
	args = append(args, "--py-files", ss.PyFiles)
	args = append(args, "--packages", ss.Packages)
	args = append(args, ss.Script)
	args = append(args, ss.SparkMaster)
	args = append(args, ss.CassandraHost)
	fmt.Println(args)
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
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

func consumeFromTopic(t TransformerSetting) {
	go func() {
		consumer := kafkaConsumer(t.Topic)
		mc := consumer.Messages()
		fmt.Println("Consuming on topic " + t.Topic)
		for true {
			m := <- mc 
			msg := strings.Split(string(m.Value), ",")
			fmt.Println(t.Topic+" received: "+msg[0])
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", "+msg[0]+", "+msg[1]+", "+msg[2])
					ss := SparkCommandBuilder.
						Packages(s.Packages).
						Script(s.Script).
						Files(s.Files).
						PyFiles(s.PyFiles).
						FileLocation(msg[0]).
						CassandraHost(cassandraHost).
						MinioHost(cassandraHost).
						TrialID(msg[1]).
						ContainerID(msg[2]).
						SparkMaster(sparkMaster).
						Build()
					cmd := constructTransformerSubmitCommand(ss)
					err := cmd.Start()
					cmd.Wait()
					if err != nil {
						panic(err)
						}
					fmt.Println("Script "+s.Script+" processed")
					close(requirementChannels[msg[1]])
					}
			}
			waitGroup.Done()
		}()
}

func waitToAnalyse(a AnalyserSetting) {
	go func () {
		for _, req := range strings.Split(a.Requirements, ",") {
				<-requirementChannels[req]
			}
		for _, s := range a.Scripts {
				fmt.Println("Analysing with "+s.Script)
				/*
				ss := SparkCommandBuilder.
						Packages(s.Packages).
						Script(s.Script).
						Files(s.Files).
						PyFiles(s.PyFiles).
						CassandraHost(cassandraHost).
						SparkMaster(sparkMaster).
						Build()		
				cmd := constructAnalyserSubmitCommand(ss)
				err := cmd.Start()
				cmd.Wait()
				if err != nil {
					panic(err)
					}
				*/
				fmt.Println("Script "+s.Script+" processed")
				}
		}()
	}

func main() {
	
	kafkaIp = "127.0.0.1"
	kafkaPort = "9092"
	sparkMaster = "local[*]"
	sparkHome = "/Users/Gabo/Downloads/spark-1.5.1-bin-hadoop2.6"
	cassandraHost = "localhost"
	minioHost = "localhost"
	/*
	kafkaIp = os.Getenv("KAFKA_IP")
	kafkaPort = os.Getenv("KAFKA_PORT")
	sparkMaster = os.Getenv("SPARK_MASTER")
	sparkHome = os.Getenv("SPARK_HOME")
	cassandraHost = os.Getenv("CASSANDRA_IP")
	minioHost = os.Getenv("MINIO_IP")
	*/
	dat, err := ioutil.ReadFile("config/config.json")
    if err != nil {
			panic(err)
			}
	err = json.Unmarshal(dat, &c)
	if err != nil {
			panic(err)
			}
	
	waitGroup = sync.WaitGroup{}
	requirementChannels = make(map[string]chan bool)
	
	for _, sett := range c.AnalysersSettings {
		for _, req := range strings.Split(sett.Requirements, ",") {
			requirementChannels[req] = make(chan bool)	
			}
		waitToAnalyse(sett)
		}
	
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	waitGroup.Wait()
	}

