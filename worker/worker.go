package worker

import (
	"fmt"
	"main/queue"
	"os"
	"path/filepath"
	"sync"
)

var recCount sync.WaitGroup
var recClose = sync.NewCond(&sync.Mutex{})

type Worker interface {
	run(inp, out, rec chan *queue.Task)
}

func NewWorker() Worker {
	return &fetcher{}
}

type fetcher struct {
}

func (item fetcher) run(inp, out, rec chan *queue.Task) {
	for currentTask := range inp {
		func() {
			defer recCount.Done()

			objects, err := readDir(currentTask.Path) // custom's changed os.ReadDir
			if err != nil {
				fmt.Printf("Objects read error. Path:%s  error:%s\n", currentTask.Path, err.Error())
				return
			}

			for _, object := range objects {
				objectPath := filepath.Join(currentTask.Path, object.Name())

				if object.IsDir() {
					recCount.Add(1)
					rec <- &queue.Task{Size: 0, Path: objectPath}
					continue
				}

				objectInfo, err := object.Info()
				if err != nil {
					fmt.Printf("Object-info read error. Path:%s  error:%s\n", currentTask.Path, err.Error())
					continue
				}
				out <- &queue.Task{Size: objectInfo.Size(), Path: objectPath}
			}
		}()
	}
}

// Implementation os.ReadDir without slices.SortFunc of entries
func readDir(name string) ([]os.DirEntry, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dirs, err := file.ReadDir(-1)
	return dirs, err
}

func Start(path string) chan *queue.Task {
	rec := make(chan *queue.Task)

	// send first task to "rec" Chan
	recCount.Add(1)
	go func() {
		rec <- &queue.Task{Size: 0, Path: path}
	}()

	// Function "Start" - owner of "rec" Chan
	// "Start" is responsible for closing this channel.
	// The channel should be closed if one of two events happens:
	// 1. All tasks are completed "recCount.Wait() - unlocked"
	// 2. The context is canceled "all workers was stoped"

	// 1.
	// We wait for the first event and send a signal about it
	go func() {
		recCount.Wait() 

		recClose.L.Lock()
		defer recClose.L.Unlock()

		recClose.Signal()
	}()

	// 2.
	// The second event can occur in the worker pool.
	// Since workers are also senders of data to the "rec" channel,
	// they also send a signal about the completion of their work
	// (when the context is canceled, for example)

	// Here we wait until one of two events happens.
	go func() {
		recClose.L.Lock()
		defer recClose.L.Unlock()

		recClose.Wait()
		close(rec)
	}()

	return rec
}

// RunPool starts a pool of workers
func RunPool(runWorker Worker, amt int, inp, out, rec chan *queue.Task) {
	var workers sync.WaitGroup // WaitGroup to track the worker goroutines themselves

	for range amt { // Create 'amt' workers
		workers.Add(1)
		go func() {
			defer workers.Done()
			runWorker.run(inp, out, rec)
		}()
	}

	// This goroutine waiting for all workers to complete and closing the main output channel
	go func(currentWorker Worker, outChan chan *queue.Task) {
		workers.Wait()
		close(outChan)

		// 2.
		// For function "Start" (owner of "rec" Chan) we send signal about second event
		// (when workers are complete or the context is canceled and "rec" Chan can be closed)
		recClose.L.Lock()
		defer recClose.L.Unlock()

		recClose.Signal()
	}(runWorker, out)
}
