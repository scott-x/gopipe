package gopipe

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// --- Helper Structures and Functions ---

// TestTask implements the gopipe.Task interface, used for testing
type TestTask struct {
	ID    int
	Steps []string
}

// Global resources to track concurrency and completion
var (
	concurrencyLimitA = 3
	concurrencyLimitB = 4
	concurrencyCount  = 0
	concurrencyMutex  sync.Mutex
	wgTest            sync.WaitGroup
)

// resetGlobalState resets global variables
func resetGlobalState() {
	concurrencyCount = 0
	wgTest = sync.WaitGroup{}
}

// processStageA simulates the processing logic for Stage A
func processStageA(task TestTask) (TestTask, error) {
	time.Sleep(time.Millisecond * 10)
	task.Steps = append(task.Steps, "A_done")
	return task, nil
}

// processStageB simulates the processing logic for Stage B
func processStageB(task TestTask) (TestTask, error) {
	time.Sleep(time.Millisecond * 5)
	task.Steps = append(task.Steps, "B_done")
	return task, nil
}

// processStageWithError simulates a stage that produces an error
func processStageWithError(task TestTask) (TestTask, error) {
	if task.ID%2 == 0 {
		return TestTask{}, errors.New("simulated error on even ID task")
	}
	task.Steps = append(task.Steps, "ErrorStage_done")
	return task, nil
}

// TestPipelinePanic tests whether the pipeline deadlocks when a stage panics
func TestPipelinePanic(t *testing.T) {
	resetGlobalState()
	const totalTasks = 5

	pipe := NewPipeline[TestTask](totalTasks).
		AddStage("PanicStage", 1, func(task TestTask) (TestTask, error) {
			panic("something went wrong")
		})

	ctx := context.Background()
	input, output := pipe.Run(ctx)

	input <- TestTask{ID: 1}
	close(input)

	done := make(chan bool)
	go func() {
		for range output {
		}
		done <- true
	}()

	select {
	case <-done:
		t.Log("Pipeline finished despite panic")
	case <-time.After(time.Second * 2):
		t.Error("Pipeline deadlocked after panic")
	}
}

// TestPipelineContextCancel tests whether the pipeline respects context cancellation
func TestPipelineContextCancel(t *testing.T) {
	resetGlobalState()
	const totalTasks = 100

	pipe := NewPipeline[TestTask](10).
		AddStage("SlowStage", 1, func(task TestTask) (TestTask, error) {
			time.Sleep(time.Millisecond * 100)
			return task, nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	input, output := pipe.Run(ctx)

	go func() {
		for i := 0; i < totalTasks; i++ {
			select {
			case <-ctx.Done():
				return
			case input <- TestTask{ID: i}:
			}
		}
	}()

	// Cancel after a short time
	time.Sleep(time.Millisecond * 50)
	cancel()

	done := make(chan bool)
	go func() {
		for range output {
		}
		done <- true
	}()

	select {
	case <-done:
		t.Log("Pipeline shutdown correctly after cancellation")
	case <-time.After(time.Second * 1):
		t.Error("Pipeline did not shutdown after cancellation")
	}
}

// --- Unit Test Cases ---

// TestPipelineFlow tests whether tasks flow completely and shut down gracefully
func TestPipelineFlow(t *testing.T) {
	resetGlobalState()
	const totalTasks = 10

	// 1. Build Pipeline: A(3 workers) -> B(4 workers)
	pipe := NewPipeline[TestTask](totalTasks).
		AddStage("StageA", concurrencyLimitA, processStageA).
		AddStage("StageB", concurrencyLimitB, processStageB)

	ctx := context.Background()
	input, output := pipe.Run(ctx)

	// 2. Produce tasks
	for i := 1; i <= totalTasks; i++ {
		input <- TestTask{ID: i, Steps: make([]string, 0)}
	}
	close(input)

	// 3. Consume output and check results
	completedTasks := 0
	wgTest.Add(1)
	go func() {
		defer wgTest.Done()
		for myTask := range output {
			completedTasks++

			// Check if the task went through all stages (2 steps)
			if len(myTask.Steps) != 2 || myTask.Steps[0] != "A_done" || myTask.Steps[1] != "B_done" {
				t.Errorf("Task %d failed processing steps: expected [A_done, B_done], got %v", myTask.ID, myTask.Steps)
			}
		}
	}()

	// 4. Wait for all Workers to exit
	pipe.Wait()
	wgTest.Wait() // Wait for the output consuming Goroutine to finish

	// 5. Final check
	if completedTasks != totalTasks {
		t.Errorf("Expected %d completed tasks, got %d", totalTasks, completedTasks)
	}
}

// TestPipelineConcurrency tests whether the concurrency limit for a stage is respected
func TestPipelineConcurrency(t *testing.T) {
	resetGlobalState()
	const totalTasks = 20

	// 1. Set max concurrency limit
	maxWorkers := 2

	// Redefine a StageFunc specifically for concurrency testing
	maxConcurrency := 0
	concurrencyTestFunc := func(task TestTask) (TestTask, error) {
		concurrencyMutex.Lock()
		concurrencyCount++
		current := concurrencyCount
		if current > maxConcurrency {
			maxConcurrency = current // Record peak concurrency
		}
		concurrencyMutex.Unlock()

		// Ensure the task takes long enough for other Workers to try entering
		time.Sleep(time.Millisecond * 50)

		concurrencyMutex.Lock()
		concurrencyCount--
		concurrencyMutex.Unlock()

		return task, nil
	}

	pipe := NewPipeline[TestTask](totalTasks).
		AddStage("StageC", maxWorkers, concurrencyTestFunc)

	ctx := context.Background()
	input, _ := pipe.Run(ctx)

	// 2. Produce tasks
	for i := 1; i <= totalTasks; i++ {
		input <- TestTask{ID: i}
	}
	close(input)

	// 3. Wait for all Workers to exit
	pipe.Wait()

	// 4. Final check: the observed peak concurrency should not exceed the set limit
	if maxConcurrency > maxWorkers {
		t.Errorf("Concurrency test failed: Expected max concurrency <= %d, got %d", maxWorkers, maxConcurrency)
	} else {
		t.Logf("Concurrency test passed. Max workers: %d, Observed peak concurrency: %d", maxWorkers, maxConcurrency)
	}
}

// TestPipelineWithError tests task flow when an error stage is included
func TestPipelineWithError(t *testing.T) {
	resetGlobalState()
	const totalTasks = 10

	// 1. Build Pipeline: A(3) -> ErrorStage(3) -> B(4)
	pipe := NewPipeline[TestTask](totalTasks).
		AddStage("StageA", 3, processStageA).
		AddStage("ErrorStage", 3, processStageWithError). // Even ID tasks will fail
		AddStage("StageB", 4, processStageB)

	ctx := context.Background()
	input, output := pipe.Run(ctx)

	// 2. Produce tasks
	for i := 1; i <= totalTasks; i++ {
		input <- TestTask{ID: i, Steps: make([]string, 0)}
	}
	close(input)

	// 3. Consume output and check results
	completedTasks := 0
	wgTest.Add(1)
	go func() {
		defer wgTest.Done()
		for myTask := range output {
			completedTasks++

			// Odd ID tasks (1, 3, 5, 7, 9) should complete all 3 steps
			expectedSteps := 3

			if myTask.ID%2 == 0 {
				t.Errorf("Even ID Task %d should have been stopped by error, but reached output", myTask.ID)
			}

			if len(myTask.Steps) != expectedSteps {
				t.Errorf("Odd ID Task %d failed processing steps: expected %d, got %d. Steps: %v", myTask.ID, expectedSteps, len(myTask.Steps), myTask.Steps)
			}
		}
	}()

	// 4. Wait for all Workers to exit
	pipe.Wait()
	wgTest.Wait()

	// 5. Final check: Only odd tasks (5 total) should have completed
	expectedCompleted := 5
	if completedTasks != expectedCompleted {
		t.Errorf("Expected %d completed tasks, got %d", expectedCompleted, completedTasks)
	}
}

// TestZeroTasks tests for graceful shutdown when zero tasks are input
func TestZeroTasks(t *testing.T) {
	resetGlobalState()

	// 1. Build Pipeline: A(1) -> B(1)
	pipe := NewPipeline[TestTask](1).
		AddStage("StageA", 1, processStageA).
		AddStage("StageB", 1, processStageB)

	ctx := context.Background()
	input, output := pipe.Run(ctx)

	// 2. Immediately close input
	close(input)

	// 3. Consume output (ensure Channel eventually closes)
	completedTasks := 0
	wgTest.Add(1)
	go func() {
		defer wgTest.Done()
		for range output {
			completedTasks++
		}
	}()

	// 4. Wait for all Workers to exit
	pipe.Wait()
	wgTest.Wait()

	// 5. Final check: No tasks should have completed
	if completedTasks != 0 {
		t.Errorf("Expected 0 completed tasks, got %d", completedTasks)
	}
}
