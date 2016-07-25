package config

import (
	"github.com/minio/minio-go"
	"strings"
	"gopkg.in/yaml.v2"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Retrieve the benchmarking configuration from Minio
func TakeBenchmarkConfigFromMinio(experimentID string) (int, string, string, string) {
	type SutStruct struct {
		Name string `yaml:"name"` 
		Version string `yaml:"version"`
		Type string `yaml:"type"`
	}
	type BenchmarkConfig struct {
		Trials int `yaml:"trials"`
		Sut SutStruct `yaml:"sut"`
	}
	
	var benchmarkConfig BenchmarkConfig
	
	// Use a secure connection.
    ssl := false
	
    // Initialize minio client object.
	minioClient, err := minio.New(MinioHost+":"+MinioPort, MinioAccessKey, MinioSecretKey, ssl)
	if err != nil {
    	panic(err)
	}
	
	// Path of the file
	path := strings.Replace(experimentID, ".", "/", -1)
	
	// Get object info
	objInfo, err := minioClient.StatObject(BenchmarksConfigBucket, path+"/"+BenchmarksConfigName)
	if err != nil {
	    panic(err)
	}
	
	// Get object
	object, err := minioClient.GetObject(BenchmarksConfigBucket, path+"/"+BenchmarksConfigName)
	if err != nil {
	    panic(err)
	}
	dat := make([]byte, objInfo.Size)
	object.Read(dat)
    
    // Unmarshal yaml
	err = yaml.Unmarshal(dat, &benchmarkConfig)
	if err != nil {
		panic(err)
	}
	
	// Return values we need
	numOfTrials := benchmarkConfig.Trials
	SUTName := benchmarkConfig.Sut.Name
	SUTVersion := benchmarkConfig.Sut.Version
	SUTType := benchmarkConfig.Sut.Type
	
	return numOfTrials, SUTName, SUTVersion, SUTType
}

