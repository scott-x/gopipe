Translate to: [简体中文](README-zh.md)

### gopipe: A Generic Go Concurrency Pipeline Utility 🚀

`gopipe` is a lightweight, generic library for building highly concurrent and scalable **pipelines** in Go, based on the **Worker Pool** and **Channel** pattern. It allows you to easily define multi-stage processing flows with configurable concurrency limits for each stage.

### ✨ Features

  * **Configurable Concurrency:** Set a precise number of `Workers` for each stage (e.g., Stage A: 3 workers, Stage B: 4 workers).
  * **Decoupled Stages:** Stages communicate asynchronously via buffered Go Channels, minimizing blocking.
  * **Generic Task Handling:** Uses the `Task` interface for maximum flexibility—any struct can be a task.
  * **Automatic Cleanup:** Manages `sync.WaitGroup` and cascading Channel closing for graceful shutdown.
  * **Chainable API:** Uses a Builder pattern (`AddStage().AddStage()`) for intuitive pipeline definition.

### 📦 Installation

```bash
go get github.com/scott-x/gopipe
```

### 🚀 Usage Example

#### 1\. Define Your Task

Your task structure must implement the `gopipe.Task` interface.

```go
package main

import (
    "fmt"
    "time"
    "github.com/scott-x/gopipe"
)

type MyTask struct {
    ID    int
    DataA string
}

func (t MyTask) GetID() int {
    return t.ID
}

func processA(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("A Worker: Processing task %d\n", myTask.ID)
    time.Sleep(time.Millisecond * 100)
    myTask.DataA = fmt.Sprintf("Task %d processed by A", myTask.ID)
    return myTask, nil
}
```

#### 2\. Build and Run the Pipeline

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

    // 6. Wait for all workers to finish
    pipe.Wait()
    fmt.Println("All pipeline tasks completed.")
}
```