Translate to: [简体中文](README-zh.md)

### gopipe: A Generic Go Concurrency Pipeline Utility 🚀

`gopipe` is a lightweight, generic library for building highly concurrent and scalable **pipelines** in Go, based on the **Worker Pool** and **Channel** pattern. It allows you to easily define multi-stage processing flows with configurable concurrency limits for each stage.

---

### ✨ Features

* **Configurable Concurrency:** Set a precise number of `Workers` for each stage (e.g., Stage A: 3 workers, Stage B: 4 workers).
* **Type Safety with Generics:** Leverage Go Generics for type-safe task processing—no more interface casting or type assertions.
* **Context Support:** Fully supports `context.Context` for easy cancellation and timeout management.
* **Automatic Cleanup:** Manages `sync.WaitGroup` and cascading Channel closing for **graceful shutdown**.
* **Panic Recovery:** Automatically recovers from worker panics to prevent pipeline deadlocks.
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

Define your task structure and processing functions. No interface implementation is required.

```go
package main

import (
    "context"
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

// Stage A: Concurrency 3
func processA(task MyTask) (MyTask, error) {
    fmt.Printf("A Worker: Processing task %d\n", task.ID)
    time.Sleep(time.Millisecond * 100)
    task.DataA = fmt.Sprintf("Task %d processed by A", task.ID)
    return task, nil
}

// Stage B: Concurrency 4 (Consumes A's output)
func processB(task MyTask) (MyTask, error) {
    fmt.Printf("B Worker: Processing task %d (DataA: %s)\n", task.ID, task.DataA)
    time.Sleep(time.Millisecond * 150)
    task.DataB = fmt.Sprintf("Task %d processed by B", task.ID)
    return task, nil
}

// Stage C: Concurrency 5 (Final stage, consumes B's output)
func processC(task MyTask) (MyTask, error) {
    fmt.Printf("C Worker: Finalizing task %d (DataB: %s)\n", task.ID, task.DataB)
    time.Sleep(time.Millisecond * 50)
    return task, nil
}
```

#### 2\. Build and Run the Pipeline

```go
func main() {
    const totalTasks = 20
    ctx := context.Background()
    
    // 1. Create Pipeline with generic type and buffer size
    pipe := gopipe.NewPipeline[MyTask](totalTasks)

    // 2. Add Stages (A: 3 workers, B: 4 workers, C: 5 workers)
    pipe.AddStage("StageA", 3, processA).
        AddStage("StageB", 4, processB).
        AddStage("StageC", 5, processC) 

    // 3. Run and get input/output channels
    inputCh, outputCh := pipe.Run(ctx)

    // 4. Input tasks
    for i := 1; i <= totalTasks; i++ {
        inputCh <- MyTask{ID: i}
    }

    // 5. Close the input channel to signal shutdown
    close(inputCh)

    // 6. Consume the final output
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