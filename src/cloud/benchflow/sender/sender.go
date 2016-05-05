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
var reqTracker = make(map[string] map[string] bool)
var reqGroupDone = make(map[string] map[string] bool)
var allRequirements []string
var allScripts []string

var kafkaIp string
var kafkaPort string
var sparkMaster string
var sparkHome string
var cassandraHost string
var minioHost string
var waitGroup sync.WaitGroup

var pysparkCassandraVersion = "0.3.5"
var analysersPath = "/Users/Gabo/benchflow/analysers/analysers"
var transformersPath = "/Users/Gabo/benchflow/data-transformers/data-transformers"

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
	Scripts []TransformerScript `yaml:"scripts"`
}

type TransformerScript struct {
	Script string `yaml:"script"`
	Files string `yaml:"files"`
	PyFiles string `yaml:"py_files"`
	Packages string `yaml:"packages"`
	}

type AnalyserScript struct {
	ScriptName string `yaml:"script_name"`
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

func constructTransformerSubmitArguments(ss SparkSubmit, experimentID string) []string {
	var args []string
	args = append(args, "--jars", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--conf", "spark.cassandra.connection.host="+os.Getenv("CASSANDRA_IP"))
	args = append(args, "--driver-class-path", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--py-files", transformersPath+"/transformations/dataTransformations.py"+","+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--files", ss.Files)
	args = append(args, "--master", ss.SparkMaster)
	// TODO: Move this in a configuration
	args = append(args, "--conf", "spark.driver.memory=4g")
	args = append(args, ss.Script)
	args = append(args, ss.MinioHost)
	args = append(args, ss.FileLocation)
	args = append(args, ss.TrialID)
	args = append(args, experimentID)
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
						Files(transformersPath+"/conf/data-transformers/"+msg.SUT_name+".data-transformers.yml").
						PyFiles(s.PyFiles).
						FileLocation("runs/"+k).
						CassandraHost(cassandraHost).
						MinioHost(minioHost).
						TrialID(msg.Trial_id).
						SUTName(msg.SUT_name).
						ContainerID(msg.Collector_name).
						SparkMaster(sparkMaster).
						Build()
					args := constructTransformerSubmitArguments(ss, msg.Experiment_id)
					submitScript(args, s.Script)
					containerID := msg.Collector_name
					meetRequirement(t.Topic, msg.Trial_id, msg.Experiment_id, "trial")
					launchAnalyserScripts(msg.Trial_id, msg.Experiment_id, msg.SUT_name, msg.Total_trials_num, containerID, msg.Collector_name)
					}
				}
			consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}

func submitAnalyser(scriptName string, script string, trialID string, experimentID string, SUTName string, containerID string, level string) {
	var args []string
	args = append(args, "--jars", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--driver-class-path", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--conf", "spark.cassandra.connection.host="+os.Getenv("CASSANDRA_IP"))
	args = append(args, "--files", analysersPath+"/conf/analysers/"+SUTName+".analysers.yml")
	args = append(args, "--py-files", analysersPath+"/commons/commons.py,"+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--master", "local[*]")
	args = append(args, script)
	args = append(args, trialID)
	args = append(args, experimentID)
	args = append(args, SUTName)
	args = append(args, containerID)
	fmt.Println(args)
	success := submitScript(args, script)
	if success {
		meetRequirement(scriptName, trialID, experimentID, level)
	}
}

func checkRequirements(neededReqsString string, currentReqsString map[string]bool) bool {
	reqMet := true
	neededReqs := strings.Split(neededReqsString, ",")
	fmt.Println(neededReqsString)
	fmt.Println(currentReqsString)
	for _, nr := range neededReqs {
		if _, ok := currentReqsString[nr]; !ok {
			reqMet = false
			break
		}
	}
	return reqMet
}

func meetRequirement(req string, trialID string, experimentID string, level string) {
	if level == "trial" {
		if _, ok := reqTracker[trialID]; !ok {
			reqTracker[trialID] = make(map[string]bool)
		}
		reqTracker[trialID][req] = true
		}
	if level == "experiment" {
		if _, ok := reqTracker[experimentID]; !ok {
			reqTracker[experimentID] = make(map[string]bool)
		}
		reqTracker[experimentID][req] = true
		}
	fmt.Println(reqTracker[trialID])
	}

func isTrialComplete(trialID string) bool{
	for _, sc := range allScripts {
		if _, ok := reqTracker[trialID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+trialID+" done")
	delete(reqTracker, trialID)
	fmt.Println(reqTracker)
	return true
	}

func isExperimentComplete(experimentID string) bool{
	for _, sc := range allScripts {
		if _, ok := reqTracker[experimentID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+experimentID+" done")
	delete(reqTracker, experimentID)
	fmt.Println(reqTracker)
	return true
	}

func launchAnalyserScripts(trialID string, experimentID string, SUTName string, totalTrials int, containerID string, collectorName string) {
	currentReqs := reqTracker[trialID]
	for r, scripts := range reqScripts {
		fmt.Println("Checking for: "+r)
		groupAlreadyDone := false
		if _, ok := reqGroupDone[trialID][r]; ok {
			groupAlreadyDone = true
		}
		reqMet := checkRequirements(r, currentReqs)
		if reqMet && !groupAlreadyDone {
			fmt.Println("ALL REQUIREMENTS MET FOR: "+r)
			if _, ok := reqGroupDone[trialID]; !ok {
				reqGroupDone[trialID] = make(map[string]bool)
			}
			reqGroupDone[trialID][r] = true
			var wg sync.WaitGroup
			wg.Add(len(scripts))
			for _, sc := range scripts {
				go func(sc AnalyserScript) {
					defer wg.Done()
					submitAnalyser(sc.ScriptName, sc.TrialScript, trialID, experimentID, SUTName, containerID, "trial")
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
						submitAnalyser(sc.ScriptName, sc.ExperimentScript, trialID, experimentID, SUTName, containerID, "experiment")
						}
					}(sc)
				}
			wg.Wait()
			isExperimentComplete(experimentID)
			isTrialComplete(trialID)
			launchAnalyserScripts(trialID, experimentID, SUTName, totalTrials, containerID, collectorName)
			}
		}
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
	
	dat, err := ioutil.ReadFile("configuration/config2.yml")
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
		reqs := strings.Split(s.Requirements, ",")
		for _, r := range reqs {
			allRequirements = append(allRequirements, r)
			}
		for _, sc := range s.Scripts {
			allScripts = append(allScripts, sc.ScriptName)
			}
		}
	
	fmt.Println(allRequirements)
	
	waitGroup = sync.WaitGroup{}
	
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	waitGroup.Wait()
	}

