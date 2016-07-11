package scripts

import (
	"encoding/json"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Function that constructs and returns the arguments for the Spark configuration
func constructSparkArguments() []string {
	var args []string
	args = append(args, "--master", SparkMaster)
	args = append(args, "--jars", SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	args = append(args, "--driver-memory", DriverMemory)
	args = append(args, "--executor-memory", ExecutorMemory)
    args = append(args, "--conf", "spark.executor.heartbeatInterval="+ExecutorHeartbeatInterval)
    args = append(args, "--conf", "spark.storage.blockManagerSlaveTimeoutMs="+BlockManagerSlaveTimeoutMs)
    args = append(args, "--conf", "spark.core.connection.ack.wait.timeout="+AckWaitTimeout)
	args = append(args, "--conf", "spark.cassandra.connection.host="+CassandraHost)
	args = append(args, "--driver-class-path", SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	return args
}

// Function that constructs and returns the arguments for a spark-submit command for a transformer script
func ConstructTransformerSubmitArguments(s TransformerScript, msg KafkaMessage, containerID string, hostID string, SUTName string, SUTVersion string) []string {
	var args []string
	args = constructSparkArguments()
	args = append(args, "--py-files", TransformersPath+"/commons/commons.py"+","+TransformersPath+"/transformations/dataTransformations.py"+","+SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	args = append(args, "--files", ConfigurationsPath+"/data-transformers/"+SUTName+".data-transformers.yml")
	args = append(args, s.Script)
	transformerArguments := TransformerArguments{}
	transformerArguments.Cassandra_keyspace = CassandraKeyspace
	transformerArguments.Container_ID = msg.Container_id
	transformerArguments.Experiment_ID = msg.Experiment_id
	transformerArguments.File_bucket = RunsBucket
	transformerArguments.File_path = msg.Minio_key
	transformerArguments.Host_ID = msg.Host_id
	transformerArguments.Minio_access_key = MinioAccessKey
	transformerArguments.Minio_host = MinioHost
	transformerArguments.Minio_port = MinioPort
	transformerArguments.Minio_secret_key = MinioSecretKey
	transformerArguments.SUT_Name = SUTName
	transformerArguments.SUT_Version = SUTVersion
	transformerArguments.Trial_ID = msg.Trial_id
	jsonArg, _ := json.Marshal(transformerArguments)
	args = append(args, string(jsonArg))
	return args
}

// Function that constructs the arguments for a spark-submit comand for an analyser script
func ConstructAnalyserSubmitArguments(scriptName string, script string, trialID string, experimentID string, SUTName string, SUTVersion string, containerID string, hostID string) []string {
	var args []string
	args = constructSparkArguments()
	args = append(args, "--files", ConfigurationsPath+"/analysers/"+SUTName+".analysers.yml")
	args = append(args, "--py-files", AnalysersPath+"/commons/commons.py,"+SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	args = append(args, script)
	analyserArguments := AnalyserArguments{}
	analyserArguments.Cassandra_keyspace = CassandraKeyspace
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