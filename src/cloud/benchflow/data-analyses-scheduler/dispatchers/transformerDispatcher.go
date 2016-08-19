package dispatchers

import (
	"fmt"
	"cloud/benchflow/data-analyses-scheduler/workers"
	. "cloud/benchflow/data-analyses-scheduler/vars"
)

// Queue of workers, and the queue of work
var TransformerWorkerQueue chan chan WorkRequest
// The size is set to twice the number of tranformer workers, in order to not take too many messages from Kafka when the workers are not ready
// to launch scripts
var TransformerWorkQueue = make(chan WorkRequest, NTransformerWorkers*2)

// Starts the dispatcher
func StartTransformerDispatcher(nworkers int) {
  // Initialises the queue
  TransformerWorkerQueue = make(chan chan WorkRequest, nworkers)
  
  // Starts the workers
  for i := 0; i<nworkers; i++ {
    fmt.Println("Starting transformer worker", i+1)
    worker := workers.NewTransformerWorker(i+1, TransformerWorkerQueue)
    worker.Start()
  }
  
   // Obtains work from the work queue, and passes the work to the first available worker in the worker queue
  // (note that the workers add themselves to the worker queue)
  go func() {
    for {
      select {
      case work := <-TransformerWorkQueue:
        fmt.Println("Received work request for script "+work.ScriptName+", "+work.SUTName+", "+work.SUTVersion+", "+work.TrialID+", "+work.ContainerID+", "+work.HostID+", "+work.Level)
        go func() {
          worker := <-TransformerWorkerQueue
          
          fmt.Println("Dispatching work request for script "+work.ScriptName+", "+work.SUTName+", "+work.SUTVersion+", "+work.TrialID+", "+work.ContainerID+", "+work.HostID+", "+work.Level)
          worker <- work
        }()
      }
    }
  }()
}