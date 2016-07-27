package main

import (
    "fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"sync"
	"strings"
	"github.com/spf13/viper"
	"cloud/benchflow/data-analyses-scheduler/consumers"
	"cloud/benchflow/data-analyses-scheduler/dispatchers"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Main function, which registers configurations and starts the consumers
func main() {
	// Settings for viper
	viper.SetConfigName("configuration")
	viper.AddConfigPath(AppPath+"/")
	viper.AddConfigPath("./")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
	    panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	
	// Getting app configuration with viper, uses ENV Variables instead of set
	CassandraKeyspace = viper.GetString("cassandra_keyspace")
	KafkaIp = viper.GetString("kafka_host")
	KafkaPort = viper.GetString("kafka_port")
	CassandraHost = viper.GetString("cassandra_host")
	DriverMemory = viper.GetString("driver_memory")
	ExecutorMemory = viper.GetString("executor_memory")
	ExecutorHeartbeatInterval = viper.GetString("executor_heartbeat_interval")
	BlockManagerSlaveTimeoutMs = viper.GetString("block_manager_slave_timeout_ms")
	AckWaitTimeout = viper.GetString("ack_wait_timeout")
	MinioHost = viper.GetString("minio_host")
	MinioPort = viper.GetString("minio_port")
	MinioAccessKey = viper.GetString("minio_access_key")
	MinioSecretKey = viper.GetString("minio_secret_key")
	RunsBucket = viper.GetString("runs_bucket")
	SparkHome = viper.GetString("spark_home")
	SparkMaster = viper.GetString("spark_master")
	SparkPort = viper.GetString("spark_port")
	Alluxio_port = viper.GetString("alluxio_port")
	PysparkCassandraVersion = viper.GetString("pyspark_cassandra_version")
	AppPath = viper.GetString("app_path")
	AnalysersPath = viper.GetString("analysers_path")
	TransformersPath = viper.GetString("transformers_path")
	TransformersConfigurationsPath = viper.GetString("transformers_configurations_path")
	TransformersConfigurationFileName = viper.GetString("transformers_configuration_file_name")
	AnalysersConfigurationsPath = viper.GetString("analysers_configurations_path")
	AnalysersConfigurationFileName = viper.GetString("analysers_configuration_file_name")
	TestsConfigBucket = viper.GetString("tests_config_bucket")
	TestsConfigName = viper.GetString("tests_config_name")
	
	NTransformerWorkers = viper.GetInt("transformer_workers")
	NAnalyserWorkers = viper.GetInt("analyser_workers")
	
	// Getting dependencies configuration and unmarshaling in defined structures
	dat, err := ioutil.ReadFile("./configuration/analysers.scheduler.configuration.yml")
    if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(dat, &C)
	if err != nil {
		panic(err)
	}
	
	dat, err = ioutil.ReadFile("./configuration/data-transformers.scheduler.configuration.yml")
    if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(dat, &C)
	if err != nil {
		panic(err)
	}
	
	// Mapping the requirements string to the scripts associated with those requirements
	for _, s := range C.AnalysersSettings {
		ReqScripts[s.Requirements] = s.Scripts
		reqs := strings.Split(s.Requirements, ",")
		for _, r := range reqs {
			AllRequirements = append(AllRequirements, r)
		}
		for _, sc := range s.Scripts {
			AllScripts = append(AllScripts, sc.ScriptName)
		}
	}
	
	// Starts the wait group
	WaitGroup = sync.WaitGroup{}
	
	// Start the dispatchers.
	dispatchers.StartAnalyserDispatcher(NTransformerWorkers)
	dispatchers.StartTransformerDispatcher(NAnalyserWorkers)
	
	// Starts consumers
	consumers.StartAnalyserConsumer()
	for _, sett := range C.TransformersSettings {
		consumers.StartDataTransformerConsumer(sett)
		WaitGroup.Add(1)
	}
	
	// Waits as long as consumers are running
	WaitGroup.Wait()
}