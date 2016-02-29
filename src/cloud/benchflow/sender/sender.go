package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/streamrail/concurrent-map"
	"gopkg.in/yaml.v2"
	"os/exec"
	"os"
	"os/signal"
	"sync"
	//"strings"
	"time"
)

//var trialCount = make(map[string] int)
var trialCount = cmap.New()

var c Configuration
var reqScripts = make(map[string] []AnalyserScript)

var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string
var minioHost string
var waitGroup sync.WaitGroup

//var mutex = &sync.Mutex{}

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
	Total_trials_num int `json:"total_trials_num"`
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
				fmt.Println("Received invalid json: " + string(m.Value))
				continue
				}
			fmt.Println(t.Topic+" received: "+msg.Minio_key)
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+msg.Minio_key+", trial id: "+msg.Trial_id)
					ss := SparkCommandBuilder.
						Packages(s.Packages).
						Script(s.Script).
						//Files(s.Files).
						//Files("/Users/Gabo/benchflow/spark-tasks-sender/conf/data-transformers/"+msg.SUT_name+".data-transformers.yml").
						Files("/app/data-transformers/conf/data-transformers/"+msg.SUT_name+".data-transformers.yml").
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
					launchAnalyserScripts(msg.Trial_id, msg.Experiment_id, msg.Total_trials_num, t.Topic, msg.Minio_key)
					}
				consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}

func submitAnalyser(script string, trialID string, minioKey string) {
	var args []string
	args = append(args, "--master", "local[*]")
	args = append(args, "--packages", "TargetHolding:pyspark-cassandra:0.2.2")
	args = append(args, script)
	args = append(args, "local[*]")
	args = append(args, os.Getenv("CASSANDRA_IP"))
	args = append(args, trialID)
	args = append(args, minioKey)
	fmt.Println(args)
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	cmd.Wait()
	if err != nil {
		panic(err)
		}
	fmt.Println("Script "+script+" processed")
	}

func launchAnalyserScripts(trialID string, experimentID string, totalTrials int, req string, minioKey string) {
	/*
	var scripts []AnalyserScript
	for _, s := range c.AnalysersSettings {
		if s.Requirements == req {
				scripts = s.Scripts
				break
			}
		}
	*/
	for _, s := range reqScripts[req] {
		go func(sc AnalyserScript) {
			submitAnalyser(sc.TrialScript, trialID, minioKey)
			//mutex.Lock()
			counterId := experimentID+"_"+sc.TrialScript
			/*
			_, present := trialCount[counterId]
			if(present) {
				trialCount[counterId] += 1
				} else {
				trialCount[counterId] = 1
				}
			if(trialCount[counterId] == totalTrials) {
				// Launch Experiment metric
				fmt.Printf("All trials "+sc.TrialScript+" for experiment "+experimentID+" completed, launching experiment analyser")
				submitAnalyser(sc.ExperimentScript, trialID)
				}
			*/
			trialCount.SetIfAbsent(counterId, 0)
			i, ok := trialCount.Get(counterId)
			if ok {
				trialCount.Set(counterId, i.(int)+1)
				}
			i, ok = trialCount.Get(counterId)
			if ok && i.(int) == totalTrials {
				trialCount.Remove(counterId)
				// Launch Experiment metric
				fmt.Printf("All trials "+sc.TrialScript+" for experiment "+experimentID+" completed, launching experiment analyser")
				submitAnalyser(sc.ExperimentScript, trialID, minioKey)
				}
			//mutex.Unlock()
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
	
	for _, s := range c.AnalysersSettings {
		reqScripts[s.Requirements] = s.Scripts
		}
	
	waitGroup = sync.WaitGroup{}
	
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	waitGroup.Wait()
	}

