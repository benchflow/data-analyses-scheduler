package scripts

import (
	"fmt"
	"os/exec"
	"os"
	"strings"
	"bytes"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Function that submits a script with spark-submit and checks the output for errors
func SubmitScript(args []string, script string) bool {
	retries := 0
	cmd := exec.Command(SparkHome+"/bin/spark-submit", args...)
	for retries < 3 {
		retries += 1
		cmd.Stdout = os.Stdout
		errOutput := &bytes.Buffer{}
		cmd.Stderr = errOutput
		err := cmd.Start()
		cmd.Wait()
		if err != nil {
			panic(err)
			}
		errLog := errOutput.String()
		fmt.Println(errLog)
		if checkForErrors(errLog) {
			fmt.Println("Script " + script + " failed")
			fmt.Println(errLog)
			continue
		}
		fmt.Println("Script "+script+" processed")
		break
	}
	if retries == 3 {
		fmt.Println("Max number of retries reached for " + script)
		return false
		}
	return true
	}

// Checks if the output log for spark-submit contains errors
func checkForErrors(errLog string) bool {
	errString := strings.ToLower(errLog)
	if strings.Contains(errString, "error") {
		return true
		}
	if strings.Contains(errString, "exception") {
		return true
		}
	return false
	}