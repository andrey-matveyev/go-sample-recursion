package worker

import (
	"context"
	"fmt"
	"main/queue"
	"os"
	"path/filepath"
	"sync"
)

var recCount sync.WaitGroup

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

func Start1(path string) chan *queue.Task {
	out := make(chan *queue.Task)
	recCount.Add(1)
	go func() {
		out <- &queue.Task{Size: 0, Path: path}
	}()
	go func() {
		recCount.Wait()
		close(out)
	}()
	return out
}

func Start(ctx context.Context, path string) chan *queue.Task {
	out := make(chan *queue.Task)

	recCount.Add(1)
	go func() {
		select {
		case <-ctx.Done():
			return
		case out <- &queue.Task{Size: 0, Path: path}:
		}
	}()

	go func() {
		defer close(out)

		doneWaiting := make(chan struct{})
		go func() {
			recCount.Wait()
			close(doneWaiting)
		}()

		select {
		case <-ctx.Done():
			fmt.Printf("Start: Context cancelled, stopping wait for recCount: %v\n", ctx.Err())
			return
		case <-doneWaiting:
			fmt.Println("Start: All recursive tasks completed naturally.")
			return
		}
	}()

	return out
}

func RunPool(runWorker Worker, amt int, inp, out, rec chan *queue.Task) {
	var workers sync.WaitGroup

	for range amt {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runWorker.run(inp, out, rec)
		}()
	}

	go func(currentWorker Worker, outChan chan *queue.Task) {
		workers.Wait()
		close(outChan)
	}(runWorker, out)
}
