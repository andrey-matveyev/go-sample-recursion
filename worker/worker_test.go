package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"main/queue"
)

// createTestDir creates a temporary test directory structure and files
func createTestDir(t *testing.T, basePath string) {
	// Create the root directory for the test
	rootDir := filepath.Join(basePath, "root_dir")
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		t.Fatalf("Failed to create root_dir: %v", err)
	}

	// Create file1.txt (10 bytes)
	if err := os.WriteFile(filepath.Join(rootDir, "file1.txt"), []byte("0123456789"), 0644); err != nil {
		t.Fatalf("Failed to create file1.txt: %v", err)
	}

	// Create subdir1
	subdir1 := filepath.Join(rootDir, "subdir1")
	if err := os.MkdirAll(subdir1, 0755); err != nil {
		t.Fatalf("Failed to create subdir1: %v", err)
	}

	// Create file2.txt (20 bytes)
	if err := os.WriteFile(filepath.Join(subdir1, "file2.txt"), []byte("abcdefghijklmnopqrst"), 0644); err != nil {
		t.Fatalf("Failed to create file2.txt: %v", err)
	}

	// Create empty_dir inside subdir1
	emptyDir := filepath.Join(subdir1, "empty_dir")
	if err := os.MkdirAll(emptyDir, 0755); err != nil {
		t.Fatalf("Failed to create empty_dir: %v", err)
	}

	// Create subdir2
	subdir2 := filepath.Join(rootDir, "subdir2")
	if err := os.MkdirAll(subdir2, 0755); err != nil {
		t.Fatalf("Failed to create subdir2: %v", err)
	}

	// Create file3.txt (30 bytes)
	if err := os.WriteFile(filepath.Join(subdir2, "file3.txt"), []byte("012345678901234567890123456789"), 0644); err != nil {
		t.Fatalf("Failed to create file3.txt: %v", err)
	}

	// Create file4.txt (52 bytes)
	if err := os.WriteFile(filepath.Join(subdir2, "file4.txt"), []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"), 0644); err != nil {
		t.Fatalf("Failed to create file4.txt: %v", err)
	}
}

// TestFullPipelineTraversal tests the entire recursive traversal pipeline.
// This is the most appropriate way to test fetcher.run, given its dependencies.
func TestFullPipelineTraversal(t *testing.T) {
	// Create a temporary directory for the test, which will be automatically removed after the test.
	testDir := t.TempDir()
	testRootPath := filepath.Join(testDir, "root_dir") // The path from which traversal will start
	createTestDir(t, testDir)                          // Create test data

	// Expected files with their sizes
	expectedFiles := map[string]int64{
		filepath.Join(testRootPath, "file1.txt"):            10,
		filepath.Join(testRootPath, "subdir1", "file2.txt"): 20,
		filepath.Join(testRootPath, "subdir2", "file3.txt"): 30,
		filepath.Join(testRootPath, "subdir2", "file4.txt"): 52,
	}

	// Initialize context to manage the program's lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure the context is cancelled after the test completes

	// Start the pipeline:

	// rec - the channel through which worker.Start will provide initial and "recursive" tasks
	rec := Start(testRootPath) // Start traversal from our test root directory

	// inp - the channel from which workers will read tasks. It is the output of your queue,
	// which, in turn, reads tasks from the 'rec' channel.
	inp := queue.OutQueue(ctx, queue.InpQueue(rec))

	// out - the final channel for receiving traversal results (file information)
	out := make(chan *queue.Task) // This channel will be passed to RunPool, which will close it.

	// Launch the worker pool
	// Using 2 workers for parallel processing, mimicking the example in main.go
	RunPool(NewWorker(), 2, inp, out, rec)

	// Collect results from the final 'out' channel
	collectedFiles := make(map[string]int64)
	for fileTask := range out {
		collectedFiles[fileTask.Path] = fileTask.Size
	}

	// Assertions
	// 1. Check the number of files found
	if len(collectedFiles) != len(expectedFiles) {
		t.Errorf("Mismatch in number of files found. Expected %d, Got %d. Actual files: %+v",
			len(expectedFiles), len(collectedFiles), collectedFiles)
		// For more detailed output if the count doesn't match
		for path := range expectedFiles {
			if _, ok := collectedFiles[path]; !ok {
				t.Errorf("Missing expected file: %s", path)
			}
		}
		for path := range collectedFiles {
			if _, ok := expectedFiles[path]; !ok {
				t.Errorf("Unexpected file found: %s", path)
			}
		}
	}

	// 2. Compare collected files with expected ones (path and size)
	for expectedPath, expectedSize := range expectedFiles {
		gotSize, ok := collectedFiles[expectedPath]
		if !ok {
			// Already printed above if the count didn't match
			continue
		}
		if gotSize != expectedSize {
			t.Errorf("Size mismatch for %s: Expected %d, Got %d", expectedPath, expectedSize, gotSize)
		}
	}

	// --- Additional tests for different scenarios ---

	t.Run("Empty directory traversal", func(t *testing.T) {
		// Create a separate temporary directory for this subtest
		emptyTestDir := t.TempDir()
		emptyPath := filepath.Join(emptyTestDir, "empty_sub")
		if err := os.MkdirAll(emptyPath, 0755); err != nil {
			t.Fatalf("Failed to create empty test dir: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		recEmpty := Start(emptyPath)
		inpEmpty := queue.OutQueue(ctx, queue.InpQueue(recEmpty))
		outEmpty := make(chan *queue.Task)
		RunPool(NewWorker(), 1, inpEmpty, outEmpty, recEmpty) // 1 worker for simplicity

		collectedEmptyFiles := make(map[string]int64)
		for fileTask := range outEmpty {
			collectedEmptyFiles[fileTask.Path] = fileTask.Size
		}

		if len(collectedEmptyFiles) != 0 {
			t.Errorf("Expected 0 files from empty directory, got %d: %+v", len(collectedEmptyFiles), collectedEmptyFiles)
		}
	})

	t.Run("Non-existent directory traversal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		nonExistentPath := filepath.Join(t.TempDir(), "definitely_not_exist") // Guaranteed non-existent path

		// The Start function will still add 1 to recCount and send the task
		recNonExistent := Start(nonExistentPath)
		inpNonExistent := queue.OutQueue(ctx, queue.InpQueue(recNonExistent))
		outNonExistent := make(chan *queue.Task)
		RunPool(NewWorker(), 1, inpNonExistent, outNonExistent, recNonExistent)

		collectedNonExistentFiles := make(map[string]int64)
		for fileTask := range outNonExistent {
			collectedNonExistentFiles[fileTask.Path] = fileTask.Size
		}

		if len(collectedNonExistentFiles) != 0 {
			t.Errorf("Expected 0 files from non-existent directory, got %d: %+v", len(collectedNonExistentFiles), collectedNonExistentFiles)
		}
		// Note: fetcher.run will print an error to the console for non-existent paths,
		// but it won't send anything to 'out' or 'rec', other than decrementing the counter.
		// This test confirms that no files are emitted.
	})
}

func TestPipelineCancellation(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()
	// Create a deeper structure for cancellation to occur mid-traversal
	testRootPath := filepath.Join(testDir, "root_dir")
	createTestDir(t, testRootPath) // Create the initial structure

	// Add a few more levels and files to increase the likelihood of interruption
	currentPath := testRootPath
	for i := 0; i < 3; i++ { // 3 additional levels of nesting
		currentPath = filepath.Join(currentPath, fmt.Sprintf("level_%d", i))
		if err := os.MkdirAll(currentPath, 0755); err != nil {
			t.Fatalf("Failed to create nested dir: %v", err)
		}
		for j := 0; j < 5; j++ { // 5 files at each level
			if err := os.WriteFile(filepath.Join(currentPath, fmt.Sprintf("file_%d.txt", j)), []byte(fmt.Sprintf("content_%d_%d", i, j)), 0644); err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short period.
	// The timing might need adjustment depending on your system's speed.
	cancelAfter := 1 * time.Millisecond // 50 milliseconds
	go func() {
		time.Sleep(cancelAfter)
		cancel() // Cancel the context
	}()

	// Start the pipeline exactly as in main.go, but pass the cancellable context
	rec := Start(testRootPath)
	inp := queue.OutQueue(ctx, queue.InpQueue(rec))
	out := make(chan *queue.Task)
	RunPool(NewWorker(), 2, inp, out, rec) // Use 2 workers

	// Collect results until the 'out' channel closes
	collectedFiles := make(map[string]int64)
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for fileTask := range out {
			collectedFiles[fileTask.Path] = fileTask.Size
		}
	}()

	// Wait for the file collector to finish (the 'out' channel closes)
	collectWg.Wait()

	// Assertions for context cancellation:

	// 1. Ensure that the context was indeed cancelled.
	select {
	case <-ctx.Done():
		// Success: context was cancelled.
		t.Logf("Context was successfully cancelled with error: %v", ctx.Err())
	case <-time.After(cancelAfter + 100*time.Millisecond): // Give some buffer time
		t.Fatal("Context was not cancelled as expected within the timeout.")
	}

	// 2. Check that the pipeline did not hang and the 'out' channel closed correctly.
	//    The fact that the `for fileTask := range out` loop completed already guarantees this.
	//    If there were a deadlock, the test would hang (and Go's test timeout would trigger).
	//    The number of collected files should be less than the total expected,
	//    unless the cancellation occurred too late.
	t.Logf("Collected %d files during cancelled traversal.", len(collectedFiles))

	// TODO: More complex checks could be added:
	// - That there were no console errors other than the expected "context cancelled".
	// - That all goroutines have exited (difficult to check directly).

	// The main point: the test should complete without hanging, which means the pipeline
	// shut down correctly. If `recCount.Wait()` were blocking, the test would hang.
}
