切换到: [English](README.md)

### gopipe: 通用 Go 并发流水线工具 🚀

`gopipe` 是一个轻量级、通用的 Go 语言库，用于构建高并发、可扩展的**流水线（Pipeline）**。它基于 **Worker Pool（协程池）**和 **Channel（通道）**模式，让你能够轻松定义多级处理流程，并为每个工序配置固定的并发度限制。



---

### ✨ 特性

* **可配置并发度:** 为每个工序（Stage）设置精确的 `Worker` 数量（例如，工序 A：3 个 Worker，工序 B：4 个 Worker）。
* **工序解耦:** 各工序通过带缓冲区的 **Go Channel** 进行异步通信，最大程度减少阻塞。
* **通用任务处理:** 使用 **`Task` 接口**实现，具备最大的灵活性，任何结构体都可以作为任务。
* **自动化清理:** 自动管理 `sync.WaitGroup` 和级联的 Channel 关闭，实现**优雅停机**。
* **链式 API:** 使用 **Builder 模式** (`AddStage().AddStage()`)，定义流水线流程直观简洁。

---

### 📦 安装

```bash
go get github.com/scott-x/gopipe
````

-----

### 🚀 使用示例

本示例演示了一个三道工序（A、B、C）的流水线，并发限制分别为 3、4 和 5 个 Worker。

#### 1\. 定义任务和工序函数

你的任务结构体必须实现 `gopipe.Task` 接口。我们将定义完整的 `processA`、`processB` 和 `processC` 逻辑。

```go
package main

import (
    "fmt"
    "time"
    "github.com/scott-x/gopipe"
)

// 任务结构体定义
type MyTask struct {
    ID    int
    DataA string // A 工序的结果
    DataB string // B 工序的结果
}

func (t MyTask) GetID() int {
    return t.ID
}

// 工序 A: 并发 3
func processA(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("A Worker: 正在处理任务 %d\n", myTask.ID)
    time.Sleep(time.Millisecond * 100)
    myTask.DataA = fmt.Sprintf("任务 %d 已被 A 处理", myTask.ID)
    return myTask, nil
}

// 工序 B: 并发 4 (消费 A 的输出)
func processB(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("B Worker: 正在处理任务 %d (DataA: %s)\n", myTask.ID, myTask.DataA)
    time.Sleep(time.Millisecond * 150)
    myTask.DataB = fmt.Sprintf("任务 %d 已被 B 处理", myTask.ID)
    return myTask, nil
}

// 工序 C: 并发 5 (最终工序，消费 B 的输出)
func processC(task gopipe.Task) (gopipe.Task, error) {
    myTask := task.(MyTask) 
    fmt.Printf("C Worker: 正在进行最终处理任务 %d (DataB: %s)\n", myTask.ID, myTask.DataB)
    time.Sleep(time.Millisecond * 50)
    // 返回 myTask，将其发送到最终的 output channel
    return myTask, nil
}
```

#### 2\. 构建和运行流水线

在 `main` 函数中，我们启动一个 Goroutine 来消费 `outputCh`，避免 "declared but not used" 的编译错误。

```go
func main() {
    const totalTasks = 20
    
    // 1. 创建 Pipeline 实例，设置 Channel 缓冲区大小
    pipe := gopipe.NewPipeline(totalTasks)

    // 2. 添加工序 (A: 3 worker, B: 4 worker, C: 5 worker)
    pipe.AddStage("StageA", 3, processA).
        AddStage("StageB", 4, processB).
        AddStage("StageC", 5, processC) 

    // 3. 运行并获取输入/输出 Channel
    inputCh, outputCh := pipe.Run()

    // 4. 生产任务
    for i := 1; i <= totalTasks; i++ {
        inputCh <- MyTask{ID: i}
    }

    // 5. 关键步骤: 关闭输入 Channel，以信号通知流水线开始关闭
    close(inputCh)

    // 6. 消费最终输出 (解决 'outputCh declared and not used' 错误)
    go func() {
        completedCount := 0
        for range outputCh {
            completedCount++
        }
        fmt.Printf("Main: 从输出通道收集到 %d 个已完成任务。\n", completedCount)
    }()

    // 7. 等待所有 Worker 完成
    pipe.Wait()
    fmt.Println("所有流水线任务已完成。主程序退出。")
}
```