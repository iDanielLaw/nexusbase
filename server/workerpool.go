package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
)

// Job represents a task to be executed by a worker.
// It encapsulates the data needed for the task and a channel to return the result.
type Job struct {
	Ctx      context.Context
	Data     interface{} // Can be core.DataPoint or []core.DataPoint
	ResultCh chan error
}

// WorkerPool manages a pool of workers to process jobs concurrently.
type WorkerPool struct {
	numWorkers int
	jobQueue   chan Job
	engine     engine.StorageEngineInterface
	logger     *slog.Logger
	wg         sync.WaitGroup
}

// NewWorkerPool creates a new worker pool.
// numWorkers: the number of worker goroutines to spawn.
// queueSize: the size of the job queue.
func NewWorkerPool(numWorkers, queueSize int, eng engine.StorageEngineInterface, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		numWorkers: numWorkers,
		jobQueue:   make(chan Job, queueSize),
		engine:     eng,
		logger:     logger,
	}
}

// Start launches the worker goroutines.
func (wp *WorkerPool) Start() {
	for i := 1; i <= wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
	wp.logger.Info("Worker pool started", "num_workers", wp.numWorkers)
}

// Stop gracefully shuts down the worker pool.
// It closes the job queue and waits for all workers to finish their current jobs.
func (wp *WorkerPool) Stop() {
	close(wp.jobQueue)
	wp.wg.Wait()
	wp.logger.Info("Worker pool stopped")
}

// worker is the main loop for a single worker goroutine.
// It continuously fetches jobs from the queue and processes them.
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for job := range wp.jobQueue {
		var err error
		if points, ok := job.Data.([]core.DataPoint); ok {
			err = wp.engine.PutBatch(job.Ctx, points)
		} else {
			err = fmt.Errorf("unknown job type in worker pool: %T", job.Data)
		}
		job.ResultCh <- err
	}
}
