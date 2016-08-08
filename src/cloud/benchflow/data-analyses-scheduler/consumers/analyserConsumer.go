package consumers

import (
	"fmt"
	"cloud/benchflow/data-analyses-scheduler/scripts"
	"cloud/benchflow/data-analyses-scheduler/dispatchers"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

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
				for r, scripts := range ReqScripts {
					submitWorksThatMeetReqs(request, r, scripts)
					submitWorksThatMeetTrialCount(request, r, scripts)
				}
			}
		}
	}()
}

// Submit the work when requirements are met
func submitWorksThatMeetReqs(request WorkRequest, r string, analyserScripts []AnalyserScript) {
	currentReqs := ReqTracker[request.TrialID]
	reqMet := scripts.CheckRequirements(r, currentReqs)
	if reqMet {
		fmt.Println("All requirements met for: "+r)
		for _, sc := range analyserScripts {
			args := scripts.ConstructAnalyserSubmitArguments(sc.ScriptName, sc.TrialScript, request.TrialID, request.ExperimentID, request.SUTName, request.SUTVersion, request.ContainerID, request.ContainerName, request.HostID)
			dispatchers.AnalyserWorkQueue <- WorkRequest{SparkArgs: args, ScriptName: sc.ScriptName, Script: sc.TrialScript, Topic: request.Topic, TrialID: request.TrialID, ExperimentID: request.ExperimentID, ContainerID: request.ContainerID, ContainerName: request.ContainerName, HostID: request.HostID, SUTName: request.SUTName, SUTVersion: request.SUTVersion, TotalTrialsNum: request.TotalTrialsNum, CollectorName: request.CollectorName, Level: "trial"}
  			fmt.Println("Analyser work request queued")
		}
	}
}

// Submit the work when trial counter of a script matches the total num of trials for an experiment
func submitWorksThatMeetTrialCount(request WorkRequest, r string, analyserScripts []AnalyserScript) {
	for _, sc := range analyserScripts {
		counterId := scripts.GetCounterID(request.ExperimentID, sc.ScriptName, request.ContainerName, request.HostID)
		i, ok := TrialCount.Get(counterId)
		if ok && i.(int) == request.TotalTrialsNum {
			TrialCount.Remove(counterId)
			fmt.Printf("All trials "+sc.TrialScript+" for experiment "+request.ExperimentID+" completed")
			args := scripts.ConstructAnalyserSubmitArguments(sc.ScriptName, sc.ExperimentScript, request.TrialID, request.ExperimentID, request.SUTName, request.SUTVersion, request.ContainerID, request.ContainerName, request.HostID)
			dispatchers.AnalyserWorkQueue <- WorkRequest{SparkArgs: args, ScriptName: sc.ScriptName, Script: sc.ExperimentScript, Topic: request.Topic, TrialID: request.TrialID, ExperimentID: request.ExperimentID, ContainerID: request.ContainerID, ContainerName: request.ContainerName, HostID: request.HostID, SUTName: request.SUTName, SUTVersion: request.SUTVersion, TotalTrialsNum: request.TotalTrialsNum, CollectorName: request.CollectorName, Level: "experiment"}
  			fmt.Println("Analyser work request queued")
		}
	}
}