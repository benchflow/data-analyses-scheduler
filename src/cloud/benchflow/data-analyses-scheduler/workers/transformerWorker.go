package workers

import (
 	"fmt"
 	"cloud/benchflow/data-analyses-scheduler/scripts"
 	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Create new worker
func NewTransformerWorker(id int, workerQueue chan chan WorkRequest) TransformerWorker {
  worker := TransformerWorker{
    ID:          id,
    Work:        make(chan WorkRequest),
    WorkerQueue: workerQueue,
    QuitChan:    make(chan bool)}
  
  return worker
}

// Worker struct
type TransformerWorker struct {
  ID          int
  Work        chan WorkRequest
  WorkerQueue chan chan WorkRequest
  QuitChan    chan bool
}

// Start worker
func (w *TransformerWorker) Start() {
    go func() {
      for {
      	// Worker adds itself to the worker queue
        w.WorkerQueue <- w.Work
        
        select {
    	// Takes work from its own queue
        case work := <-w.Work:
          // Submits script to Spark
          success := scripts.SubmitScript(work.SparkArgs, work.Script)
          if success {
			  scripts.MeetRequirement(work.Topic, work.TrialID, work.ExperimentID, "trial")
			  AnalyserDispatchRequestsQueue <- work
		  }
          
        // Quits if signalled
        case <-w.QuitChan:
          fmt.Printf("worker%d stopping\n", w.ID)
          return
        }
      }
    }()
}

// Stop worker
func (w *TransformerWorker) Stop() {
  go func() {
    w.QuitChan <- true
  }()
}