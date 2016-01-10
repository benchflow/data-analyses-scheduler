package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"os/exec"
	"os"
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

type KafkaMessage struct {
	Minio_key string `json:"minio_key"`
	Trial_id string `json:"trial_id"`
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
	partConsumer, err := consumer.ConsumePartition(name, 0, sarama.OffsetOldest)
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
			//msg := strings.Split(string(m.Value), ",")
			var msg KafkaMessage
			err := json.Unmarshal(m.Value, &msg)
			if err != nil {
				panic(err)
				}
			fmt.Println(t.Topic+" received: "+msg.Minio_key)
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+msg.Minio_key+", trial id: "+msg.Trial_id)
					ss := SparkCommandBuilder.
						Packages(s.Packages).
						Script(s.Script).
						Files(s.Files).
						PyFiles(s.PyFiles).
						FileLocation(msg.Minio_key).
						CassandraHost(cassandraHost).
						MinioHost(cassandraHost).
						TrialID(msg.Trial_id).
						// TODO: Retrieve real container ID
						ContainerID("00cc9619-66a1-9e11-e594-91c8e0eb1859").
						SparkMaster(sparkMaster).
						Build()
					cmd := constructTransformerSubmitCommand(ss)
					err := cmd.Start()
					cmd.Wait()
					if err != nil {
						panic(err)
						}
					fmt.Println("Script "+s.Script+" processed")
					launchAllScripts(msg.Trial_id)
					//close(requirementChannels[msg.Trial_id])
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
				fmt.Println("Script "+s.Script+" processed")
				}
		}()
	}

// TODO: Hardcoded for testing
func launchAllScripts(trialID string) {
	scripts := []string{"/app/analysers/processDuration.py", "/app/analysers/cpu.py", "/app/analysers/ram.py", "/app/analysers/numberOfProcessInstances.py"}
	for _, s := range scripts {
			var args []string
			args = append(args, "--master", "local[*]")
			args = append(args, "--packages", "TargetHolding:pyspark-cassandra:0.2.2")
			args = append(args, s)
			args = append(args, "local[*]")
			args = append(args, os.Getenv("CASSANDRA_IP"))
			args = append(args, trialID)
			fmt.Println(args)
			cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
			err := cmd.Start()
			cmd.Wait()
			if err != nil {
				panic(err)
				}
			fmt.Println("Script "+s+" processed")
		}
	}

func main() {
	kafkaIp = os.Getenv("KAFKA_IP")
	kafkaPort = os.Getenv("KAFKA_PORT")
	sparkMaster = os.Getenv("SPARK_MASTER")
	sparkHome = os.Getenv("SPARK_HOME")
	cassandraHost = os.Getenv("CASSANDRA_IP")
	minioHost = os.Getenv("MINIO_IP")
	
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
		//waitToAnalyse(sett)
		}
	
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	waitGroup.Wait()
	}

