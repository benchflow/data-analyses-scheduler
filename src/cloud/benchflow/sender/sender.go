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
	"github.com/spf13/viper"
)

// Concurrent map for counting how many trials have been performed for a certain experiment
var trialCount = cmap.New()

// Configuration of the scripts and dependencies
var c Configuration

// Map that connects the string representing script requirements to the list of scripts that have those requirement as per configuration
var reqScripts = make(map[string] []AnalyserScript)

// Nested map that tracks for a given trialID and requirement if said requirement was met for that trial (if contains true, requirement is met)
// Example: reqGroupDone[trial_1][cpu] = true
var reqTracker = make(map[string] map[string] bool)

// Nested map that tracks which group of scripts was launched for a given trial, indexed by trial and requirements string. 
// Example: reqGroupDone[trial_1][cpu,ram] = true
var reqGroupDone = make(map[string] map[string] bool)

// Lists of all requirements
var allRequirements []string

//List of all scripts
var allScripts []string

// Variables from configuration of the app
var cassandraKeyspace string
var kafkaIp string
var kafkaPort string
var cassandraHost string
var minioHost string
var minioPort string
var minioAccessKey string
var minioSecretKey string
var runsBucket string
var driverMemory string
var executorMemory string
var executorHeartbeatInterval string
var blockManagerSlaveTimeoutMs string
var ackWaitTimeout string
var sparkHome string
var sparkMaster string
var sparkPort string
var alluxio_port string
var pysparkCassandraVersion string
var analysersPath string
var transformersPath string
var configurationsPath string
var numOfTrials int
var SUTName string
var SUTVersion string

// Sync group to prevent the app from terminating as long as consumers are listening on kafka
var waitGroup sync.WaitGroup

// Structures for storing the dependencies configurations
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
	Minio_key string `json:"minio_key"`
	Trial_id string `json:"trial_id"`
	Experiment_id string `json:"experiment_id"`
	Container_id string `json:"container_id"`
	Host_id string `json:"host_id"`
	Collector_name string `json:"collector_name"`
	}

type TransformerArguments struct {
	Cassandra_keyspace string `json:"cassandra_keyspace"`
    Minio_host string `json:"minio_host"`
    Minio_port string `json:"minio_port"`
    Minio_access_key string `json:"minio_access_key"`
    Minio_secret_key string `json:"minio_secret_key"`
    File_bucket string `json:"file_bucket"`
    File_path string `json:"file_path"`
    Trial_ID string `json:"trial_id"`
    Experiment_ID string `json:"experiment_id"`
    SUT_Name string `json:"sut_name"`
    SUT_Version string `json:"sut_version"`
    Container_ID string `json:"container_id"`
    Host_ID string `json:"host_id"`
	}

type AnalyserArguments struct {
	Cassandra_keyspace string `json:"cassandra_keyspace"`
    Trial_ID string `json:"trial_id"`
    Experiment_ID string `json:"experiment_id"`
    SUT_Name string `json:"sut_name"`
    SUT_Version string `json:"sut_version"`
    Container_ID string `json:"container_id"`
    Host_ID string `json:"host_id"`
	}

// Function that constructs and returns the arguments for the Spark configuration
func constructSparkArguments() []string {
	var args []string
	args = append(args, "--master", sparkMaster)
	args = append(args, "--jars", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--driver-memory", driverMemory)
	args = append(args, "--executor-memory", executorMemory)
    args = append(args, "--conf", "spark.executor.heartbeatInterval="+executorHeartbeatInterval)
    args = append(args, "--conf", "spark.storage.blockManagerSlaveTimeoutMs="+blockManagerSlaveTimeoutMs)
    args = append(args, "--conf", "spark.core.connection.ack.wait.timeout="+ackWaitTimeout)
	args = append(args, "--conf", "spark.cassandra.connection.host="+cassandraHost)
	args = append(args, "--driver-class-path", sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	return args
}

// Function that constructs and returns the arguments for a spark-submit command for a transformer script
func constructTransformerSubmitArguments(s TransformerScript, msg KafkaMessage, containerID string, hostID string) []string {
	var args []string
	args = constructSparkArguments()
	args = append(args, "--py-files", transformersPath+"/commons/commons.py"+","+transformersPath+"/transformations/dataTransformations.py"+","+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, "--files", configurationsPath+"/data-transformers/"+SUTName+".data-transformers.yml")
	args = append(args, s.Script)
	transformerArguments := TransformerArguments{}
	transformerArguments.Cassandra_keyspace = cassandraKeyspace
	transformerArguments.Container_ID = msg.Container_id
	transformerArguments.Experiment_ID = msg.Experiment_id
	transformerArguments.File_bucket = runsBucket
	transformerArguments.File_path = msg.Minio_key
	transformerArguments.Host_ID = msg.Host_id
	transformerArguments.Minio_access_key = minioAccessKey
	transformerArguments.Minio_host = minioHost
	transformerArguments.Minio_port = minioPort
	transformerArguments.Minio_secret_key = minioSecretKey
	transformerArguments.SUT_Name = SUTName
	transformerArguments.SUT_Version = SUTVersion
	transformerArguments.Trial_ID = msg.Trial_id
	jsonArg, _ := json.Marshal(transformerArguments)
	args = append(args, string(jsonArg))
	fmt.Println(args)
	return args
}

// Function that creates a kafka consumer on a given topic name
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

// Function that consumes messages from a topic and processes the messages, launching the transformer scripts and launching dependency checking
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
			containerIds := strings.Split(msg.Container_id, ",")
			hostIds := strings.Split(msg.Host_id, ",")
			for i, k := range minioKeys {
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+k+", trial id: "+msg.Trial_id)
					containerID := containerIds[i]
					hostID := hostIds[i]
					args := constructTransformerSubmitArguments(s, msg, containerID, hostID)
					submitScript(args, s.Script)
					meetRequirement(t.Topic, msg.Trial_id, msg.Experiment_id, "trial")
					launchAnalyserScripts(msg.Trial_id, msg.Experiment_id, SUTName, numOfTrials, containerID, hostID, msg.Collector_name)
					}
				}
			consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}

// Function that constructs the arguments for a spark-submit comand for an analyser script
func constructAnalyserSubmitArguments(scriptName string, script string, trialID string, experimentID string, SUTName string, containerID string, hostID string) []string {
	var args []string
	args = constructSparkArguments()
	args = append(args, "--files", configurationsPath+"/analysers/"+SUTName+".analysers.yml")
	args = append(args, "--py-files", analysersPath+"/commons/commons.py,"+sparkHome+"/pyspark-cassandra-assembly-"+pysparkCassandraVersion+".jar")
	args = append(args, script)
	analyserArguments := AnalyserArguments{}
	analyserArguments.Cassandra_keyspace = cassandraKeyspace
	analyserArguments.Container_ID = containerID
	analyserArguments.Experiment_ID = experimentID
	analyserArguments.Host_ID = hostID
	analyserArguments.SUT_Name = SUTName
	analyserArguments.SUT_Version = SUTVersion
	analyserArguments.Trial_ID = trialID
	jsonArg, _ := json.Marshal(analyserArguments)
	args = append(args, string(jsonArg))
	fmt.Println(args)
	return args
	}

// Function that submits an analyser script, and meets its requirement if it succeeds
func submitAnalyser(scriptName string, script string, trialID string, experimentID string, SUTName string, containerID string, hostID string, level string) {
	args := constructAnalyserSubmitArguments(scriptName, script, trialID, experimentID, SUTName, containerID, hostID)
	success := submitScript(args, script)
	if success {
		meetRequirement(scriptName, trialID, experimentID, level)
	}
}

// Function that checks if the given requirements are met
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

// Function that registers a given requirement as met
func meetRequirement(req string, trialID string, experimentID string, level string) {
	if level == "trial" {
		if _, ok := reqTracker[trialID]; !ok {
			reqTracker[trialID] = make(map[string]bool)
		}
		reqTracker[trialID][req] = true
	} else if level == "experiment" {
		if _, ok := reqTracker[experimentID]; !ok {
			reqTracker[experimentID] = make(map[string]bool)
		}
		reqTracker[experimentID][req] = true
		}
	fmt.Println(reqTracker[trialID])
	}

// Function that checks if all scripts for a given trial have been concluded
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

// Function that checks if all scripts for a given experiment have been concluded
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

// Function that checks for requirements and launches the analysers that meet them
func launchAnalyserScripts(trialID string, experimentID string, SUTName string, totalTrials int, containerID string, hostID string, collectorName string) {
	for r, scripts := range reqScripts {
		fmt.Println("Checking for: "+r)
		groupAlreadyDone := false
		if _, ok := reqGroupDone[trialID][r]; ok {
			groupAlreadyDone = true
		}
		currentReqs := reqTracker[trialID]
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
					submitAnalyser(sc.ScriptName, sc.TrialScript, trialID, experimentID, SUTName, containerID, hostID, "trial")
					counterId := experimentID+"_"+sc.TrialScript+"_"+containerID+"_"+hostID+"_"+collectorName
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
						submitAnalyser(sc.ScriptName, sc.ExperimentScript, trialID, experimentID, SUTName, containerID, hostID, "experiment")
						}
					}(sc)
				}
			wg.Wait()
			trialComplete := isTrialComplete(trialID)
			experimentComplete := isExperimentComplete(experimentID)
			if (trialComplete || experimentComplete) {
				return
				}
			launchAnalyserScripts(trialID, experimentID, SUTName, totalTrials, containerID, hostID, collectorName)
			}
		}
	}

// Function that submits a script with spark-submit and checks the output for errors
func submitScript(args []string, script string) bool {
	retries := 0
	cmd := exec.Command(sparkHome+"/bin/spark-submit", args...)
	for retries < 3 {
		retries += 1
		cmd.Stdout = os.Stdout
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

// Checks if the output log for spark-submit contains errors
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

// Main function, which registers configurations and starts the consumers
func main() {
	// Settings for viper
	viper.SetConfigName("configuration")
	viper.AddConfigPath("/app/")
	viper.AddConfigPath("./")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
	    panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	
	// Getting app configuration with viper, uses ENV Variables instead of set
	cassandraKeyspace = viper.GetString("cassandra_keyspace")
	kafkaIp = viper.GetString("kafka_host")
	kafkaPort = viper.GetString("kafka_port")
	cassandraHost = viper.GetString("cassandra_host")
	driverMemory = viper.GetString("driver_memory")
	executorMemory = viper.GetString("executor_memory")
	executorHeartbeatInterval = viper.GetString("executor_heartbeat_interval")
	blockManagerSlaveTimeoutMs = viper.GetString("block_manager_slave_timeout_ms")
	ackWaitTimeout = viper.GetString("ack_wait_timeout")
	minioHost = viper.GetString("minio_host")
	minioPort = viper.GetString("minio_port")
	minioAccessKey = viper.GetString("minio_access_key")
	minioSecretKey = viper.GetString("minio_secret_key")
	runsBucket = viper.GetString("runs_bucket")
	sparkHome = viper.GetString("spark_home")
	sparkMaster = viper.GetString("spark_master")
	sparkPort = viper.GetString("spark_port")
	alluxio_port = viper.GetString("alluxio_port")
	pysparkCassandraVersion = viper.GetString("pyspark_cassandra_version")
	analysersPath = viper.GetString("analysers_path")
	transformersPath = viper.GetString("transformers_path")
	configurationsPath = viper.GetString("configurations_path")
	// TODO: Take from the right file on Minio
	numOfTrials = viper.GetInt("num_of_trials")
	SUTName = viper.GetString("sut_name")
	SUTVersion = viper.GetString("sut_version")
	
	// Getting dependencies configuration and unmarshaling in defined structures
	dat, err := ioutil.ReadFile("/app/configuration/scripts-configuration.yml")
    if err != nil {
			panic(err)
			}
	err = yaml.Unmarshal(dat, &c)
	if err != nil {
			panic(err)
			}
	
	// Mapping the requirements string to the scripts associated with those requirements
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
	fmt.Println(reqScripts["stats"])
	
	// Starts the wait group
	waitGroup = sync.WaitGroup{}
	
	// Starts consumers
	for _, sett := range c.TransformersSettings {
		consumeFromTopic(sett)
		waitGroup.Add(1)
		}
	
	// Waits as long as consumers are running
	waitGroup.Wait()
	}

