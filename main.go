package main

import (
	"context"
	"fmt"
	"main/queue"
	"main/worker"
)

func main() {
	// Initialize context to manage the entire pipeline
	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *queue.Task)

	rec := worker.Start(".")

	inp := queue.OutQueue(mainCtx, queue.InpQueue(rec))

	worker.RunPool(worker.NewWorker(), 2, inp, out, rec)

	for task := range out {
		fmt.Printf("%d %s\n", task.Size, task.Path)
	}
}
