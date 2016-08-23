package scripts

import (
	"fmt"
	"os/exec"
	"os"
	"strings"
	//"bytes"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Copies the environment and alters the PYSPARK_PYTHON var to use the correct python path
func getCustomEnvironment(script string) []string {
	pythonPath := ""
	if strings.Contains(script, "analysers") {
		pythonPath = os.Getenv("CONDA_PYTHON")
	} else if strings.Contains(script, "data-transformers") {
		pythonPath = os.Getenv("REGULAR_PYTHON")
	}
	fmt.Println(os.Getenv("PATH"))
	env := os.Environ()
	for i, e := range(env) {
		if strings.Contains(e, "PYSPARK_PYTHON") {
			env = append(env[:i], env[i+1:]...)
			break
		}	
	}
	env = append(env, fmt.Sprintf("PYSPARK_PYTHON=%s", pythonPath))
	return env
}

// Function that submits a script with spark-submit and checks the output for errors
func SubmitScript(args []string, script string) bool {
	retries := 0
	env := getCustomEnvironment(script)
	cmd := exec.Command(SparkHome+"/bin/spark-submit", args...)
	cmd.Env = env
	for retries < 3 {
		retries += 1
		out, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("Script "+script+" exited with a fatal error")
			fmt.Println(out)
			fmt.Println(err)
			return false
		}
		fmt.Println(out)
		if checkForErrors(string(out)) {
			fmt.Println("Script " + script + " exited with an error")
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
	if strings.Contains(errString, "errno") {
		return true
	}
	//TODO: Use a better way to recognise errors
	if strings.Contains(errString, "traceback") {
		return true
	}
	return false
}