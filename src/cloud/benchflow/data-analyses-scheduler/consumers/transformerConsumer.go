package consumers

import (
 	"fmt"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"os"
	"os/signal"
	"strings"
	"time"
	"cloud/benchflow/data-analyses-scheduler/config"
	"cloud/benchflow/data-analyses-scheduler/scripts"
	"cloud/benchflow/data-analyses-scheduler/dispatchers"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)


// Function that creates a kafka consumer on a given topic name
func kafkaConsumer(name string) consumergroup.ConsumerGroup {
	config := consumergroup.NewConfig()
	config.ClientID = "benchflow"
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	consumer, err := consumergroup.JoinConsumerGroup(name+"SparkTasksSenderGroup", []string{name}, []string{KafkaIp+":"+KafkaPort}, config)
	if err != nil {
		panic("Could not connect to kafka")
		}
	return *consumer
	}

// Function that consumes messages from a topic and processes the messages
func StartDataTransformerConsumer(t TransformerSetting) {
	go func() {
		consumer := kafkaConsumer(t.Topic)
		cInterruption := make(chan os.Signal, 1)
		signal.Notify(cInterruption, os.Interrupt)
		go func() {
			<-cInterruption
			if err := consumer.Close(); err != nil {
				sarama.Logger.Println("Error closing the consumer", err)
			}
		}()
		mc := consumer.Messages()
		fmt.Println("Consuming on topic " + t.Topic)
		for true {
			m := <- mc
			var msg KafkaMessage
			fmt.Println("Received message: "+string(m.Value))
			err := json.Unmarshal(m.Value, &msg)
			if err != nil {
				fmt.Println("Received invalid json: " + string(m.Value))
				consumer.CommitUpto(m)
				continue
				}
			numOfTrials, SUTName, SUTVersion, SUTType, err := config.TakeTestConfigFromMinio(msg.Experiment_id)
			if err != nil {
				fmt.Println("Cannot retrieve benchflow file from Minio for experiment "+msg.Experiment_id)
				fmt.Println(err)
				consumer.CommitUpto(m)
				continue
			}
			minioKeys := strings.Split(msg.Minio_key, ",")
			containerIds := strings.Split(msg.Container_id, ",")
			containerNames := strings.Split(msg.Container_name, ",")
			for i, k := range minioKeys {
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+k+", trial id: "+msg.Trial_id)
					containerID := containerIds[i]
					containerName := containerNames[i]
					hostID := msg.Host_id
					args := scripts.ConstructTransformerSubmitArguments(s, msg, containerID, containerName, hostID, SUTName, SUTVersion, SUTType)
					work := WorkRequest{SparkArgs: args, Script: s.Script, ScriptName: t.Topic, Topic: t.Topic, TrialID: msg.Trial_id, ExperimentID: msg.Experiment_id, ContainerID: containerID, ContainerName: containerName, HostID: msg.Host_id, SUTName: SUTName, SUTVersion: SUTVersion, TotalTrialsNum: numOfTrials, CollectorName: msg.Collector_name, Level: "trial"}
					dispatchers.TransformerWorkQueue <- work
  					fmt.Println("Transformer work request queued for script "+work.ScriptName+", "+work.SUTName+", "+work.SUTVersion+", "+work.TrialID+", "+work.ContainerID+", "+work.HostID+", "+work.Level)
					}
				}
			consumer.CommitUpto(m)
			}
		consumer.Close()
		WaitGroup.Done()
		}()
}