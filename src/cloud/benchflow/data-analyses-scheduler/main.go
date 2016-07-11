package main

import (
    "fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"sync"
	"strings"
	"github.com/spf13/viper"
)

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
	benchmarksConfigBucket = viper.GetString("benchmarks_config_bucket")
	benchmarksConfigName = viper.GetString("benchmarks_config_name")
	
	transformerWorkers := viper.GetInt("transformer_workers")
	analyserWorkers := viper.GetInt("analyser_workers")
	
	// Getting dependencies configuration and unmarshaling in defined structures
	dat, err := ioutil.ReadFile("./configuration/analyser-scripts-configuration.yml")
    if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(dat, &c)
	if err != nil {
		panic(err)
	}
	
	dat, err = ioutil.ReadFile("./configuration/data-transformer-scripts-configuration.yml")
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
	
	// Starts the wait group
	waitGroup = sync.WaitGroup{}
	
	// Start the dispatchers.
	StartAnalyserDispatcher(transformerWorkers)
	StartTransformerDispatcher(analyserWorkers)
	
	// Starts consumers
	StartAnalyserConsumer()
	for _, sett := range c.TransformersSettings {
		StartDataTransformerConsumer(sett)
		waitGroup.Add(1)
		}
	
	// Waits as long as consumers are running
	waitGroup.Wait()
	}

