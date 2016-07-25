package vars

import (
	"github.com/streamrail/concurrent-map"
	"sync"
)

// The queue for the analyser consumer. Here the workers send their completed work requests to signal the consumer to check requirements and
// send new work accordingly.
var AnalyserDispatchRequestsQueue chan WorkRequest

// Concurrent map for counting how many trials have been performed for a certain experiment
var TrialCount = cmap.New()

// Configuration of the scripts and dependencies
var C Configuration

// Map that connects the string representing script requirements to the list of scripts that have those requirement as per configuration
var ReqScripts = make(map[string] []AnalyserScript)

// Nested map that tracks for a given trialID and requirement if said requirement was met for that trial (if contains true, requirement is met)
// Example: reqGroupDone[trial_1][cpu] = true
var ReqTracker = make(map[string] map[string] bool)

// Lists of all requirements
var AllRequirements []string

//List of all scripts
var AllScripts []string

// Variables from configuration of the app
var CassandraKeyspace string
var KafkaIp string
var KafkaPort string
var CassandraHost string
var MinioHost string
var MinioPort string
var MinioAccessKey string
var MinioSecretKey string
var RunsBucket string
var DriverMemory string
var ExecutorMemory string
var ExecutorHeartbeatInterval string
var BlockManagerSlaveTimeoutMs string
var AckWaitTimeout string
var SparkHome string
var SparkMaster string
var SparkPort string
var Alluxio_port string
var PysparkCassandraVersion string
var AnalysersPath string
var TransformersPath string
var TransformersConfigurationsPath string
var TransformersConfigurationFileName string
var AnalysersConfigurationsPath string
var AnalysersConfigurationFileName string
var BenchmarksConfigBucket string
var BenchmarksConfigName string

var NTransformerWorkers int
var NAnalyserWorkers int

// Sync group to prevent the app from terminating as long as consumers are listening on kafka
var WaitGroup sync.WaitGroup

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
    Config_file string `json:"config_file"`
    Container_ID string `json:"container_id"`
    Host_ID string `json:"host_id"`
	}

type AnalyserArguments struct {
	Cassandra_keyspace string `json:"cassandra_keyspace"`
    Trial_ID string `json:"trial_id"`
    Experiment_ID string `json:"experiment_id"`
    Config_file string `json:"config_file"`
    Container_ID string `json:"container_id"`
    Host_ID string `json:"host_id"`
	}

// Struct of a work request for the workers
type WorkRequest struct {
  SparkArgs []string
  Script string
  ScriptName string
  Topic string
  TrialID  string
  ExperimentID string
  ContainerID string
  HostID string
  SUTName string
  SUTVersion string
  TotalTrialsNum int
  CollectorName string
  Level string
}