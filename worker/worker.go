package worker

import (
	"fmt"
	"main/queue"
	"os"
	"path/filepath"
	"sync"
)

var foldersCount sync.WaitGroup

type worker interface {
	run(inp, out chan *queue.Task)
}

type fetcher struct {
	outFolder chan *queue.Task
}

func (item fetcher) run(inp, out chan *queue.Task) {
	for currentTask := range inp {
		func() {
			defer foldersCount.Done()

			objects, err := readDir(currentTask.Path) // custom's changed os.ReadDir
			if err != nil {
				fmt.Printf("Objects read error. Path:%s  error:%s\n", currentTask.Path, err.Error())
				return
			}

			for _, object := range objects {
				objectPath := filepath.Join(currentTask.Path, object.Name())

				if object.IsDir() {
					foldersCount.Add(1)
					item.outFolder <- &queue.Task{Size: 0, Path: objectPath}
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

// Implementation os.ReadDir withot slices.SortFunc of entries
func readDir(name string) ([]os.DirEntry, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dirs, err := file.ReadDir(-1)
	return dirs, err
}
