package main

import (
 	"fmt"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"os"
	"os/signal"
	"strings"
	"time"
)


// Function that creates a kafka consumer on a given topic name
func kafkaConsumer(name string) consumergroup.ConsumerGroup {
	config := consumergroup.NewConfig()
	config.ClientID = "benchflow"
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	consumer, err := consumergroup.JoinConsumerGroup(name+"SparkTasksSenderGroup", []string{name}, []string{kafkaIp+":"+kafkaPort}, config)
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
				continue
				}
			numOfTrials, SUTName, SUTVersion := takeBenchmarkConfigFromMinio(msg.Experiment_id)
			minioKeys := strings.Split(msg.Minio_key, ",")
			containerIds := strings.Split(msg.Container_id, ",")
			for i, k := range minioKeys {
				for _, s := range t.Scripts {
					fmt.Println(t.Topic+" topic, submitting script "+string(s.Script)+", minio location: "+k+", trial id: "+msg.Trial_id)
					containerID := containerIds[i]
					hostID := msg.Host_id
					args := constructTransformerSubmitArguments(s, msg, containerID, hostID, SUTName, SUTVersion)
					TransformerWorkQueue <- WorkRequest{SparkArgs: args, Script: s.Script, ScriptName: t.Topic, Topic: t.Topic, TrialID: msg.Trial_id, ExperimentID: msg.Experiment_id, ContainerID: msg.Container_id, HostID: msg.Host_id, SUTName: SUTName, SUTVersion: SUTVersion, TotalTrialsNum: numOfTrials, CollectorName: msg.Collector_name, Level: "trial"}
  					fmt.Println("Transformer work request queued")
					}
				}
			consumer.CommitUpto(m)
			}
		consumer.Close()
		waitGroup.Done()
		}()
}