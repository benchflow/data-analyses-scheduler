package scripts

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Function to resolve the correct configuration file path based on the SUT Name and Version
func getConfigFilePath(SUTVersion string, SUTName string, SUTType string, fileName string) string {
	// Split the version into 3 numbers (eg. 1.0.2 -> [1 0 2])
	versionNums := strings.Split(SUTVersion, ".")
	dirs, _ := ioutil.ReadDir(TransformersConfigurationsPath+"/"+SUTName)
	// Iterate over the dirs, finds the one for the version passed to the function, either a perfect match or matching a range (eg. 1.2.0-1.4.0 for 1.3.0)
    for _, dir := range dirs {
    	dirName := dir.Name()
    	if dirName == SUTVersion {
    		return TransformersConfigurationsPath+"/"+SUTName+"/"+dirName+"/"+fileName
		}
    	vRange := strings.Split(dirName, "-")
    	if len(vRange) == 2 {
    		vNumsRangeLow := strings.Split(vRange[0], ".")
    		vNumsRangeHigh := strings.Split(vRange[1], ".")
    		if versionNums[0] >= vNumsRangeLow[0] && versionNums[0] <= vNumsRangeHigh[0] && versionNums[1] >= vNumsRangeLow[1] && versionNums[1] <= vNumsRangeHigh[1] && versionNums[2] >= vNumsRangeLow[2] && versionNums[2] <= vNumsRangeHigh[2] {
    			return TransformersConfigurationsPath+"/"+SUTName+"/"+dirName+"/"+fileName
    		}
		}
    }
    return ""
}

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
func ConstructTransformerSubmitArguments(s TransformerScript, msg KafkaMessage, containerID string, hostID string, SUTName string, SUTVersion string, SUTType string) []string {
	var args []string
	args = constructSparkArguments()
	args = append(args, "--py-files", TransformersPath+"/commons/commons.py"+","+TransformersPath+"/transformations/dataTransformations.py"+","+SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	configFilePath := getConfigFilePath(SUTVersion, SUTName, SUTType, "data-transformers.configuration.yml")
	if configFilePath != "" {
		args = append(args, "--files", configFilePath)
	}
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
	transformerArguments.Config_file = TransformersConfigurationFileName
	transformerArguments.Trial_ID = msg.Trial_id
	jsonArg, _ := json.Marshal(transformerArguments)
	args = append(args, string(jsonArg))
	return args
}

// Function that constructs the arguments for a spark-submit comand for an analyser script
func ConstructAnalyserSubmitArguments(scriptName string, script string, trialID string, experimentID string, SUTName string, SUTVersion string, containerID string, hostID string) []string {
	var args []string
	args = constructSparkArguments()
	// This is disabled since we don't have analyser configurations for now
	//configFilePath := getConfigFilePath(SUTVersion, SUTName, "analysers.configuration.yml")
	//if configFilePath != "" {
	//	args = append(args, "--files", configFilePath)
	//}
	args = append(args, "--py-files", AnalysersPath+"/commons/commons.py,"+SparkHome+"/pyspark-cassandra-assembly-"+PysparkCassandraVersion+".jar")
	args = append(args, script)
	analyserArguments := AnalyserArguments{}
	analyserArguments.Cassandra_keyspace = CassandraKeyspace
	analyserArguments.Container_ID = containerID
	analyserArguments.Experiment_ID = experimentID
	analyserArguments.Host_ID = hostID
	analyserArguments.Config_file = AnalysersConfigurationFileName
	analyserArguments.Trial_ID = trialID
	jsonArg, _ := json.Marshal(analyserArguments)
	args = append(args, string(jsonArg))
	return args
	}