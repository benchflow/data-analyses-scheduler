package config

import (
	"github.com/minio/minio-go"
	"strings"
	"gopkg.in/yaml.v2"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Retrieve the test configuration from Minio
func TakeTestConfigFromMinio(experimentID string) (int, string, string, string, error) {
	type SutStruct struct {
		Name string `yaml:"name"` 
		Version string `yaml:"version"`
		Type string `yaml:"type"`
	}
	type TestConfig struct {
		Trials int `yaml:"trials"`
		Sut SutStruct `yaml:"sut"`
	}
	
	var testConfig TestConfig
	
	// Use a secure connection.
    ssl := MinioSSL
	
    // Initialize minio client object.
	minioClient, err := minio.New(MinioHost+":"+MinioPort, MinioAccessKey, MinioSecretKey, ssl)
	if err != nil {
    	return 0, "", "", "", err
	}
	
	// Path of the file
	path := strings.Replace(experimentID, ".", "/", -1)
	
	// Get object info
	objInfo, err := minioClient.StatObject(TestsConfigBucket, path+"/"+TestsConfigName)
	if err != nil {
	    return 0, "", "", "", err
	}
	
	// Get object
	object, err := minioClient.GetObject(TestsConfigBucket, path+"/"+TestsConfigName)
	if err != nil {
	    return 0, "", "", "", err
	}
	dat := make([]byte, objInfo.Size)
	object.Read(dat)
    
    // Unmarshal yaml
	err = yaml.Unmarshal(dat, &testConfig)
	if err != nil {
		return 0, "", "", "", err
	}
	
	// Return values we need
	numOfTrials := testConfig.Trials
	SUTName := testConfig.Sut.Name
	SUTVersion := testConfig.Sut.Version
	SUTType := testConfig.Sut.Type
	
	return numOfTrials, SUTName, SUTVersion, SUTType, err
}

