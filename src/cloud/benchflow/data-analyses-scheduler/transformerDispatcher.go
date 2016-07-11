package main

import (
	"fmt"
)

// Queue of workers, and the queue of work
var TransformerWorkerQueue chan chan WorkRequest
var TransformerWorkQueue = make(chan WorkRequest, 100)

// Starts the dispatcher
func StartTransformerDispatcher(nworkers int) {
  // Initialises the queue
  TransformerWorkerQueue = make(chan chan WorkRequest, nworkers)
  
  // Starts the workers
  for i := 0; i<nworkers; i++ {
    fmt.Println("Starting worker", i+1)
    worker := NewTransformerWorker(i+1, TransformerWorkerQueue)
    worker.Start()
  }
  
   // Obtains work from the work queue, and passes the work to the first available worker in the worker queue
  // (note that the workers add themselves to the worker queue)
  go func() {
    for {
      select {
      case work := <-TransformerWorkQueue:
        fmt.Println("Received work requeust")
        go func() {
          worker := <-TransformerWorkerQueue
          
          fmt.Println("Dispatching work request")
          worker <- work
        }()
      }
    }
  }()
}