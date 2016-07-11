package dispatchers

import (
	"fmt"
	"cloud/benchflow/data-analyses-scheduler/workers"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Queue of workers, and the queue of work
var AnalyserWorkerQueue chan chan WorkRequest
// The size of this is set to limit the queue to twice the number of transformer worker plus twice the number of analyser workers
var AnalyserWorkQueue = make(chan WorkRequest, 2*NTransformerWorkers+2*NAnalyserWorkers)

// Starts the dispatcher
func StartAnalyserDispatcher(nworkers int) {
  // Initialises the queue
  AnalyserWorkerQueue = make(chan chan WorkRequest, nworkers)
  
  // Starts the workers
  for i := 0; i<nworkers; i++ {
    fmt.Println("Starting worker", i+1)
    worker := workers.NewAnalyserWorker(i+1, AnalyserWorkerQueue)
    worker.Start()
  }
  
  // Obtains work from the work queue, and passes the work to the first available worker in the worker queue
  // (note that the workers add themselves to the worker queue)
  go func() {
    for {
      select {
      case work := <-AnalyserWorkQueue:
        fmt.Println("Received work requeust")
        go func() {
          worker := <-AnalyserWorkerQueue
          
          fmt.Println("Dispatching work request")
          worker <- work
        }()
      }
    }
  }()
}