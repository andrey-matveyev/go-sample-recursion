package main

import (
	"context"
	"fmt"
	"main/queue"  // Import your queue package
	"main/worker" // Import your worker package
)

func main() {
	// Initialize context to manage the program's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// rec - the channel through which worker.Start will provide initial and "recursive" tasks
	rec := worker.Start(".") // Start traversal from the current directory

	// inp - the channel from which workers will read. It is the output of your queue,
	// which, in turn, reads tasks from the 'rec' channel.
	inp := queue.OutQueue(ctx, queue.InpQueue(rec))

	// out - the final channel for receiving traversal results (file information)
	out := make(chan *queue.Task)

	// Launching the worker pool:
	// NewWorker() - creates a new instance of the task handler
	// 2 - the number of parallel workers
	// inp - input channel for workers
	// out - output channel for traversal results (files)
	// rec - channel for "recursive" tasks (subdirectories)
	worker.RunPool(worker.NewWorker(), 2, inp, out, rec)

	// Main loop: read and print results from the final 'out' channel.
	// The loop will terminate when the 'out' channel is closed.
	for task := range out {
		fmt.Printf("%d %s\n", task.Size, task.Path)
	}

	fmt.Println("Traversal completed.")
}
