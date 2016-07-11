package main

import (
	"fmt"
	"strings"
)

// Function that checks if the given requirements are met
func checkRequirements(neededReqsString string, currentReqsString map[string]bool) bool {
	reqMet := true
	neededReqs := strings.Split(neededReqsString, ",")
	for _, nr := range neededReqs {
		if _, ok := currentReqsString[nr]; !ok {
			reqMet = false
			break
		}
	}
	if reqMet {
		for _, nr := range neededReqs {
			delete(currentReqsString, nr)
		}
	}
	return reqMet
}

// Function that registers a given requirement as met
func meetRequirement(req string, trialID string, experimentID string, level string) {
	if level == "trial" {
		if _, ok := reqTracker[trialID]; !ok {
			reqTracker[trialID] = make(map[string]bool)
		}
		reqTracker[trialID][req] = true
	} else if level == "experiment" {
		if _, ok := reqTracker[experimentID]; !ok {
			reqTracker[experimentID] = make(map[string]bool)
		}
		reqTracker[experimentID][req] = true
	}
}

// Function that checks if all scripts for a given trial have been concluded
func isTrialComplete(trialID string) bool{
	for _, sc := range allScripts {
		if _, ok := reqTracker[trialID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+trialID+" done")
	delete(reqTracker, trialID)
	return true
}

// Function that checks if all scripts for a given experiment have been concluded
func isExperimentComplete(experimentID string) bool{
	for _, sc := range allScripts {
		if _, ok := reqTracker[experimentID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+experimentID+" done")
	delete(reqTracker, experimentID)
	return true
}

// Function that generates the key for the trial counter map
func getCounterID(experimentID string, scriptName string, containerID string, hostID string, collectorName string) string {
	return experimentID+"_"+scriptName+"_"+containerID+"_"+hostID+"_"+collectorName
}