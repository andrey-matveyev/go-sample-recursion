package worker

import (
	"context"
	"os"
	"path/filepath" 
	"testing"

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
	rec := Start(ctx, testRootPath) // Start traversal from our test root directory

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

		recEmpty := Start(ctx, emptyPath)
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
		recNonExistent := Start(ctx, nonExistentPath)
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

