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
	"strings"
	"bytes"
	"time"
)

var trialCount = cmap.New()
var mutex sync.Mutex

var c Configuration
var reqScripts = make(map[string] []AnalyserScript)
var reqTracker = make(map[string] string)

var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string
var minioHost string
var waitGroup sync.WaitGroup

var pysparkCassandraVersion = "0.2.7"

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
	Collector_name string `json:"collector_name"`
	}

func constructTransformerSubmitArguments(ss SparkSubmit) []string {
	var args []string
	args = append(args, "--jars", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--driver-class-path", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--py-files", ss.PyFiles+","+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--files", ss.Files)
	args = append(args, "--master", ss.SparkMaster)
	//args = append(args, "--packages", ss.Packages)
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
	return args
	}

func kafkaConsumer(name string) consumergroup.ConsumerGroup {
	config := consumergroup.NewConfig()
	config.ClientID = "benchflow"
	config.Offsets.Initial = sarama.OffsetOldest
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
			minioKeys := strings.Split(msg.Minio_key, ",")
			for _, k := range minioKeys {
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+k+", trial id: "+msg.Trial_id)
					ss := SparkCommandBuilder.
						Packages(s.Packages).
						Script(s.Script).
						//Files(s.Files).
						//Files("/Users/Gabo/benchflow/spark-tasks-sender/conf/data-transformers/"+msg.SUT_name+".data-transformers.yml").
						Files("/app/data-transformers/conf/data-transformers/"+msg.SUT_name+".data-transformers.yml").
						PyFiles(s.PyFiles).
						FileLocation("runs/"+k).
						CassandraHost(cassandraHost).
						MinioHost(minioHost).
						TrialID(msg.Trial_id).
						SUTName(msg.SUT_name).
						ContainerID(msg.Collector_name).
						SparkMaster(sparkMaster).
						Build()
					args := constructTransformerSubmitArguments(ss)
					submitScript(args, s.Script)
					//keyPortions := strings.Split(k, "/")
					//containerID := keyPortions[len(keyPortions)-1]
					//containerID = strings.Split(containerID, "_")[0]
					containerID := msg.Collector_name
					//fmt.Println(containerID)
					mutex.Lock()
					if v, ok := reqTracker[msg.Trial_id]; ok {
						reqTracker[msg.Trial_id] = v+","+t.Topic
						} else {
						reqTracker[msg.Trial_id] = t.Topic
						}
					mutex.Unlock()
					launchAnalyserScripts(msg.Trial_id, msg.Experiment_id, msg.Total_trials_num, t.Topic, containerID, msg.Collector_name)
					}
				}
			consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}

func submitAnalyser(script string, trialID string, experimentID string, containerID string, level string) {
	var args []string
	args = append(args, "--jars", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--driver-class-path", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--py-files", "/app/analysers/commons/commons.py,"+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--master", "local[*]")
	//args = append(args, "--packages", "TargetHolding:pyspark-cassandra:0.2.2")
	args = append(args, script)
	args = append(args, "local[*]")
	args = append(args, os.Getenv("CASSANDRA_IP"))
	args = append(args, trialID)
	args = append(args, containerID)
	fmt.Println(args)
	success := submitScript(args, script)
	if success {
		scriptList := strings.Split(script, "/")
		scriptName := scriptList[len(scriptList)-1]
		scriptName = strings.TrimRight(scriptName, ".py")
		mutex.Lock()
		if level == "trial" {
			if v, ok := reqTracker[trialID]; ok {
				reqTracker[trialID] = v+","+scriptName
				} else {
				reqTracker[trialID] = scriptName
				}
			}
		if level == "experiment" {
			mutex.Lock()
			if v, ok := reqTracker[experimentID]; ok {
				reqTracker[experimentID] = v+","+scriptName
				} else {
				reqTracker[experimentID] = scriptName
				}
			}
		mutex.Unlock()
	}
}

func checkRequirements(neededReqsString string, currentReqsString string, req string) bool {
	reqMet := false
	reqPresent := false
	neededReqs := strings.Split(neededReqsString, ",")
	currentReqs := strings.Split(currentReqsString, ",")
	//fmt.Println(currentReqsString)
	for _, nr := range neededReqs {
		reqMet = false
		for _, cr := range currentReqs {
			if nr == req {reqPresent = true}
			if nr == cr {reqMet = true; break}
		}
		if !reqMet {break}
	}
	return reqMet && reqPresent
}

func launchAnalyserScripts(trialID string, experimentID string, totalTrials int, req string, containerID string, collectorName string) {
	mutex.Lock()
	currentReqs := reqTracker[trialID]
	mutex.Unlock()
	for r, scripts := range reqScripts {
		reqMet := checkRequirements(r, currentReqs, req)
		if reqMet {
			fmt.Println("ALL REQUIREMENTS MET FOR: "+r)
			for _, sc := range scripts {
				go func(sc AnalyserScript) {
					submitAnalyser(sc.TrialScript, trialID, experimentID, containerID, "trial")
					counterId := experimentID+"_"+sc.TrialScript+"_"+collectorName
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
						submitAnalyser(sc.ExperimentScript, trialID, experimentID, containerID, "experiment")
						}
					scriptList := strings.Split(sc.TrialScript, "/")
					scriptName := scriptList[len(scriptList)-1]
					scriptName = strings.TrimRight(scriptName, ".py")
					launchAnalyserScripts(trialID, experimentID, totalTrials, scriptName, containerID, collectorName)
					}(sc)
				}
			}
	}
	/*
	for _, sc := range reqScripts[req] {
		go func(sc AnalyserScript) {
			submitAnalyser(sc.TrialScript, trialID, containerID)
			counterId := experimentID+"_"+sc.TrialScript+"_"+collectorName
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
				submitAnalyser(sc.ExperimentScript, trialID, containerID)
				}
			}(sc)
		}
		*/
	}

func submitScript(args []string, script string) bool {
	retries := 0
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	for retries < 3 {
		retries += 1
		cmd.Stdout = os.Stdout
		//cmd.Stderr = os.Stderr
		errOutput := &bytes.Buffer{}
		cmd.Stderr = errOutput
		err := cmd.Start()
		cmd.Wait()
		if err != nil {
			panic(err)
			}
		errLog := errOutput.String()
		fmt.Println(errLog)
		if checkForErrors(errLog) {
			fmt.Println("Script " + script + " failed")
			fmt.Println(errLog)
			continue
		}
		fmt.Println("Script "+script+" processed")
		break
	}
	if retries == 3 {
		fmt.Println("Max number of retries reached for " + script)
		return false
		}
	return true
	}

func checkForErrors(errLog string) bool {
	errString := strings.ToLower(errLog)
	if strings.Contains(errString, "error") {
		return true
		}
	if strings.Contains(errString, "exception") {
		return true
		}
	return false
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

