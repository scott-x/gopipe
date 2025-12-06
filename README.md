Translate to: [简体中文](README-zh.md)

### gopipe: A Generic Go Concurrency Pipeline Utility 🚀

`gopipe` is a lightweight, generic library for building highly concurrent and scalable **pipelines** in Go, based on the **Worker Pool** and **Channel** pattern. It allows you to easily define multi-stage processing flows with configurable concurrency limits for each stage.

---

### ✨ Features

* **Configurable Concurrency:** Set a precise number of `Workers` for each stage (e.g., Stage A: 3 workers, Stage B: 4 workers).
* **Decoupled Stages:** Stages communicate asynchronously via **buffered Go Channels**, minimizing blocking.
* **Generic Task Handling:** Uses the **`Task` interface** for maximum flexibility—any struct can be a task.
* **Automatic Cleanup:** Manages `sync.WaitGroup` and cascading Channel closing for **graceful shutdown**.
* **Chainable API:** Uses a **Builder pattern** (`AddStage().AddStage()`) for intuitive pipeline definition.

---

### 📦 Installation

```bash
go get github.com/scott-x/gopipe
````

-----

### 🚀 Usage Example

This example demonstrates a three-stage pipeline (A, B, C) with concurrency limits of 3, 4, and 5 workers, respectively.

#### 1\. Define Task and Stage Functions

Your task structure must implement the `gopipe.Task` interface. We will also define `processB` and `processC` to complete the pipeline logic.

```go
package main

import (
    "fmt"
    "time"
    "github.com/scott-x/gopipe"
)

// Task definition structure
type MyTask struct {
    ID    int
    DataA string // Result from Stage A
    DataB string // Result from Stage B
}

func (t MyTask) GetID() int {
    return t.ID
}

// Stage A: Concurrency 3
func processA(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("A Worker: Processing task %d\n", myTask.ID)
    time.Sleep(time.Millisecond * 100)
    myTask.DataA = fmt.Sprintf("Task %d processed by A", myTask.ID)
    return myTask, nil
}

// Stage B: Concurrency 4 (Consumes A's output)
func processB(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("B Worker: Processing task %d (DataA: %s)\n", myTask.ID, myTask.DataA)
    time.Sleep(time.Millisecond * 150)
    myTask.DataB = fmt.Sprintf("Task %d processed by B", myTask.ID)
    return myTask, nil
}

// Stage C: Concurrency 5 (Final stage, consumes B's output)
func processC(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("C Worker: Finalizing task %d (DataB: %s)\n", myTask.ID, myTask.DataB)
    time.Sleep(time.Millisecond * 50)
    // Return nil to indicate the task should not be passed further, 
    // or return myTask if you want it sent to the output channel.
    return myTask, nil
}
```

#### 2\. Build and Run the Pipeline

We will use the `outputCh` in a separate Goroutine to demonstrate consumption and avoid the "declared but not used" error.

```go
func main() {
    const totalTasks = 20
    
    // 1. Create Pipeline with buffer size
    pipe := gopipe.NewPipeline(totalTasks)

    // 2. Add Stages (A: 3 workers, B: 4 workers, C: 5 workers)
    pipe.AddStage("StageA", 3, processA).
        AddStage("StageB", 4, processB).
        AddStage("StageC", 5, processC) 

    // 3. Run and get input/output channels
    inputCh, outputCh := pipe.Run()

    // 4. Input tasks
    for i := 1; i <= totalTasks; i++ {
        inputCh <- MyTask{ID: i}
    }

    // 5. CRITICAL: Close the input channel to signal the pipeline to start shutting down
    close(inputCh)

    // 6. Consume the final output (This resolves the 'outputCh declared and not used' error)
    go func() {
        completedCount := 0
        for range outputCh {
            completedCount++
        }
        fmt.Printf("Main: Collected %d completed tasks from output channel.\n", completedCount)
    }()

    // 7. Wait for all workers to finish
    pipe.Wait()
    fmt.Println("All pipeline tasks completed. Main program exiting.")
}
```