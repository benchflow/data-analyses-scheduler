package main

import (
	"fmt"
)

// The queue for the analyser consumer. Here the workers send their completed work requests to signal the consumer to check requirements and
// send new work accordingly.
var AnalyserDispatchRequestsQueue chan WorkRequest

// Function that starts the analyser consumer
func StartAnalyserConsumer() {
	// Initialise the dispatch requests queue
	AnalyserDispatchRequestsQueue = make(chan WorkRequest, 100)
	go func() {
		for {	
			select {
			// Takes requests from the queue
      		case request := <-AnalyserDispatchRequestsQueue:
      			// For all requirement configurations, check if the requirements are met, and if the experiments can be launched when the trial
      			// counter reaches the number of trials of an experiment for a given script.
				for r, scripts := range reqScripts {
					submitWorksThatMeetReqs(request, r, scripts)
					submitWorksThatMeetTrialCount(request, r, scripts)
				}
			}
		}
	}()
}

// Submit the work when requirements are met
func submitWorksThatMeetReqs(request WorkRequest, r string, scripts []AnalyserScript) {
	currentReqs := reqTracker[request.TrialID]
	reqMet := checkRequirements(r, currentReqs)
	if reqMet {
		fmt.Println("All requirements met for: "+r)
		for _, sc := range scripts {
			args := constructAnalyserSubmitArguments(sc.ScriptName, sc.TrialScript, request.TrialID, request.ExperimentID, request.SUTName, request.SUTVersion, request.ContainerID, request.HostID)
			AnalyserWorkQueue <- WorkRequest{SparkArgs: args, ScriptName: sc.ScriptName, Script: sc.TrialScript, Topic: request.Topic, TrialID: request.TrialID, ExperimentID: request.ExperimentID, ContainerID: request.ContainerID, HostID: request.HostID, SUTName: request.SUTName, SUTVersion: request.SUTVersion, TotalTrialsNum: request.TotalTrialsNum, CollectorName: request.CollectorName, Level: "trial"}
  			fmt.Println("Analyser work request queued")
		}
	}
}

// Submit the work when trial counter of a script matches the total num of trials for an experiment
func submitWorksThatMeetTrialCount(request WorkRequest, r string, scripts []AnalyserScript) {
	for _, sc := range scripts {
		counterId := getCounterID(request.ExperimentID, sc.ScriptName, request.ContainerID, request.HostID, request.CollectorName)
		i, ok := trialCount.Get(counterId)
		if ok && i.(int) == request.TotalTrialsNum {
			trialCount.Remove(counterId)
			fmt.Printf("All trials "+sc.TrialScript+" for experiment "+request.ExperimentID+" completed")
			args := constructAnalyserSubmitArguments(sc.ScriptName, sc.ExperimentScript, request.TrialID, request.ExperimentID, request.SUTName, request.SUTVersion, request.ContainerID, request.HostID)
			AnalyserWorkQueue <- WorkRequest{SparkArgs: args, ScriptName: sc.ScriptName, Script: sc.ExperimentScript, Topic: request.Topic, TrialID: request.TrialID, ExperimentID: request.ExperimentID, ContainerID: request.ContainerID, HostID: request.HostID, SUTName: request.SUTName, SUTVersion: request.SUTVersion, TotalTrialsNum: request.TotalTrialsNum, CollectorName: request.CollectorName, Level: "experiment"}
  			fmt.Println("Analyser work request queued")
		}
	}
}