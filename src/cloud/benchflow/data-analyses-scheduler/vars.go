package main

import (
	"github.com/streamrail/concurrent-map"
	"sync"
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
var benchmarksConfigBucket string
var benchmarksConfigName string

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