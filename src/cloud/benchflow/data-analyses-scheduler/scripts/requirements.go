package scripts

import (
	"fmt"
	"strings"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Function that checks if the given requirements are met
func CheckRequirements(neededReqsString string, currentReqsString map[string]bool) bool {
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
func MeetRequirement(req string, trialID string, experimentID string, level string) {
	if level == "trial" {
		if _, ok := ReqTracker[trialID]; !ok {
			ReqTracker[trialID] = make(map[string]bool)
		}
		ReqTracker[trialID][req] = true
	} else if level == "experiment" {
		if _, ok := ReqTracker[experimentID]; !ok {
			ReqTracker[experimentID] = make(map[string]bool)
		}
		ReqTracker[experimentID][req] = true
	}
}

// Function that checks if all scripts for a given trial have been concluded
func IsTrialComplete(trialID string) bool{
	for _, sc := range AllScripts {
		if _, ok := ReqTracker[trialID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+trialID+" done")
	delete(ReqTracker, trialID)
	return true
}

// Function that checks if all scripts for a given experiment have been concluded
func IsExperimentComplete(experimentID string) bool{
	for _, sc := range AllScripts {
		if _, ok := ReqTracker[experimentID][sc]; !ok {
			return false
			}
		} 
	fmt.Println("All scripts for "+experimentID+" done")
	delete(ReqTracker, experimentID)
	return true
}

// Function that generates the key for the trial counter map
func GetCounterID(experimentID string, scriptName string, containerName string, hostID string, collectorName string) string {
	return experimentID+"_"+scriptName+"_"+containerName+"_"+hostID+"_"+collectorName
}