package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"gopkg.in/yaml.v2"
	"os/exec"
	"os"
	"os/signal"
	"sync"
	//"strings"
	"strconv"
	"time"
)

var trialCount map[string] int

var c Configuration
var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string
var minioHost string
var waitGroup sync.WaitGroup

var mutex = &sync.Mutex{}

type Configuration struct {
	TransformersSettings []TransformerSetting `yaml:"transformers_settings"`
	AnalysersSettings []AnalyserSetting `yaml:"analysers_settings"`
	}

type AnalyserSetting struct {
	Requirements string `yaml:"requirements"`
	Scripts []AnalyserScript `yaml:"scripts"`
}

type TransformerSetting struct {
	Topic string `yaml:"topic"`
	Scripts []Script `yaml:"scripts"`
}

type Script struct {
	Script string `yaml:"script"`
	Files string `yaml:"files"`
	PyFiles string `yaml:"py_files"`
	Packages string `yaml:"packages"`
	}

type AnalyserScript struct {
	TrialScript string `yaml:"script_trial"`
	ExperimentScript string `yaml:"script_experiment"`
	Files string `yaml:"files"`
	PyFiles string `yaml:"py_files"`
	Packages string `yaml:"packages"`
	}

type KafkaMessage struct {
	SUT_name string `json:"SUT_name"`
	SUT_version string `json:"SUT_version"`
	Minio_key string `json:"minio_key"`
	Trial_id string `json:"trial_id"`
	Experiment_id string `json:"experiment_id"`
	Total_trials_num string `json:"total_trials_num"`
	}

func constructTransformerSubmitCommand(ss SparkSubmit) exec.Cmd {
	var args []string
	args = append(args, "--master", ss.SparkMaster)
	args = append(args, "--files", ss.Files)
	args = append(args, "--py-files", ss.PyFiles)
	args = append(args, "--packages", ss.Packages)
	// TODO: Move this in a configuration
	args = append(args, "--conf", "spark.driver.memory=4g")
	args = append(args, ss.Script)
	args = append(args, ss.SparkMaster)
	args = append(args, ss.CassandraHost)
	args = append(args, ss.MinioHost)
	args = append(args, ss.FileLocation)
	args = append(args, ss.TrialID)
	args = append(args, ss.SUTName)
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
	// TODO: Move this in a configuration
	args = append(args, "--conf", "spark.driver.memory=4g")
	args = append(args, ss.Script)
	args = append(args, ss.SparkMaster)
	args = append(args, ss.CassandraHost)
	fmt.Println(args)
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	return *cmd
	}

/*
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
*/

func kafkaConsumer(name string) consumergroup.ConsumerGroup {
	config := consumergroup.NewConfig()
	config.ClientID = "benchflow"
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	consumer, err := consumergroup.JoinConsumerGroup(name+"SparkTasksSenderGroup", []string{name}, []string{kafkaIp+":"+kafkaPort}, config)
	if err != nil {
		panic("Could not connect to kafka")
		}
	return *consumer
	}

func consumeFromTopic(t TransformerSetting) {
	go func() {
		consumer := kafkaConsumer(t.Topic)
		cInterruption := make(chan os.Signal, 1)
		signal.Notify(cInterruption, os.Interrupt)
		go func() {
			<-cInterruption
			if err := consumer.Close(); err != nil {
				sarama.Logger.Println("Error closing the consumer", err)
			}
		}()
		mc := consumer.Messages()
		fmt.Println("Consuming on topic " + t.Topic)
		for true {
			m := <- mc
			var msg KafkaMessage
			fmt.Println(string(m.Value))
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
						//Files(s.Files).
						Files("/app/configuration/data-transformers/"+msg.SUT_name+".data-transformers.yml").
						PyFiles(s.PyFiles).
						FileLocation("runs/"+msg.Minio_key).
						CassandraHost(cassandraHost).
						MinioHost(minioHost).
						TrialID(msg.Trial_id).
						SUTName(msg.SUT_name).
						// TODO: Retrieve real container ID
						ContainerID("00cc9619-66a1-9e11-e594-91c8e0eb1859").
						SparkMaster(sparkMaster).
						Build()
					cmd := constructTransformerSubmitCommand(ss)
					cmd.Stdout = os.Stdout
    				cmd.Stderr = os.Stderr
					err := cmd.Start()
					cmd.Wait()
					if err != nil {
						panic(err)
						}
					fmt.Println("Script "+s.Script+" processed")
					totalTrialsNum, _ := strconv.Atoi(msg.Total_trials_num)
					launchAnalyserScripts(msg.Trial_id, msg.Experiment_id, totalTrialsNum, t.Topic)
					}
				consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}

func launchAnalyserScripts(trialID string, experimentID string, totalTrials int, req string) {
	var scripts []AnalyserScript
	for _, s := range c.AnalysersSettings {
		if s.Requirements == req {
				scripts = s.Scripts
				break
			}
		}
	for _, s := range scripts {
		go func(sc AnalyserScript) {
			var args []string
			args = append(args, "--master", "local[*]")
			args = append(args, "--packages", "TargetHolding:pyspark-cassandra:0.2.2")
			args = append(args, sc.TrialScript)
			args = append(args, "local[*]")
			args = append(args, os.Getenv("CASSANDRA_IP"))
			args = append(args, trialID)
			fmt.Println(args)
			cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
			cmd.Stdout = os.Stdout
	    	cmd.Stderr = os.Stderr
			err := cmd.Start()
			cmd.Wait()
			if err != nil {
				panic(err)
				}
			fmt.Println("Script "+sc.TrialScript+" processed")
			mutex.Lock()
			counterId := experimentID+"_"+sc.TrialScript
			_, present := trialCount[counterId]
			if(present) {
				trialCount[counterId] += 1
				} else {
				trialCount[counterId] = 1
				}
			if(trialCount[counterId] == totalTrials) {
				// Launch Experiment metric
				fmt.Printf("All trials "+sc.TrialScript+" for experiment "+experimentID+" completed, launching experiment analyser")
				var argss []string
				argss = append(argss, "--master", "local[*]")
				argss = append(argss, "--packages", "TargetHolding:pyspark-cassandra:0.2.2")
				argss = append(argss, sc.ExperimentScript)
				argss = append(argss, "local[*]")
				argss = append(argss, os.Getenv("CASSANDRA_IP"))
				argss = append(argss, trialID)
				fmt.Println(argss)
				cmd := exec.Command(sparkHome+"/bin/spark-submit", argss...)
				cmd.Stdout = os.Stdout
		    	cmd.Stderr = os.Stderr
				err := cmd.Start()
				cmd.Wait()
				if err != nil {
					panic(err)
					}
				fmt.Println("Script "+sc.TrialScript+" processed")
				}
			mutex.Unlock()
			}(s)
		}
	}

func main() {
	kafkaIp = os.Getenv("KAFKA_IP")
	kafkaPort = os.Getenv("KAFKA_PORT")
	sparkMaster = os.Getenv("SPARK_MASTER")
	sparkHome = os.Getenv("SPARK_HOME")
	cassandraHost = os.Getenv("CASSANDRA_IP")
	minioHost = os.Getenv("MINIO_IP")
	
	dat, err := ioutil.ReadFile("configuration/config.yml")
    if err != nil {
			panic(err)
			}
	
	err = yaml.Unmarshal(dat, &c)
	if err != nil {
			panic(err)
			}
	fmt.Println(c.AnalysersSettings)
	
	waitGroup = sync.WaitGroup{}
	trialCount = make(map[string] int)
	
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	waitGroup.Wait()
	}

