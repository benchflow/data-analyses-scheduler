package main

import (
	"encoding/json"
)

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
func constructTransformerSubmitArguments(s TransformerScript, msg KafkaMessage, containerID string, hostID string, SUTName string, SUTVersion string) []string {
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
	return args
}

// Function that constructs the arguments for a spark-submit comand for an analyser script
func constructAnalyserSubmitArguments(scriptName string, script string, trialID string, experimentID string, SUTName string, SUTVersion string, containerID string, hostID string) []string {
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
	return args
	}