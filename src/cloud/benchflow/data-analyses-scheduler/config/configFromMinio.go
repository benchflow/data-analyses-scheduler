package config

import (
	"github.com/minio/minio-go"
	"strings"
	"crypto/md5"
	"encoding/hex"
	"gopkg.in/yaml.v2"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// TODO: Take the commons one
const numOfHashCharacters int = 4
func hashKey(key string) string {
	hasher := md5.New()
    hasher.Write([]byte(key))
    hashString := hex.EncodeToString(hasher.Sum(nil))
	return (hashString[:numOfHashCharacters])
	}

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
	experimentIDMinioFormat := strings.Replace(experimentID, ".", "/", -1)
	// String to be hashed to the the hash in which the configuration i stored
	// This is: the experimentID but the last part of the same (.N), here converted to /N, that represent the experiment number
	lastDotIndex := strings.LastIndex(experimentID, ".")
	minioHash := experimentIDMinioFormat[:lastDotIndex]
	hash := hashKey(minioHash)
	path := hash+"/"+experimentIDMinioFormat
	
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

