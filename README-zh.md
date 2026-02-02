切换到: [English](README.md)

### gopipe: 通用 Go 并发流水线工具 🚀

`gopipe` 是一个轻量级、通用的 Go 语言库，用于构建高并发、可扩展的**流水线（Pipeline）**。它基于 **Worker Pool（协程池）**和 **Channel（通道）**模式，让你能够轻松定义多级处理流程，并为每个工序配置固定的并发度限制。



---

### ✨ 特性

* **可配置并发度:** 为每个工序（Stage）设置精确的 `Worker` 数量（例如，工序 A：3 个 Worker，工序 B：4 个 Worker）。
* **泛型类型安全:** 利用 Go 泛型（Generics）实现类型安全的任务处理，告别接口转换和类型断言。
* **Context 支持:** 全面支持 `context.Context`，便于实现取消（Cancellation）和超时管理。
* **自动化清理:** 自动管理 `sync.WaitGroup` 和级联的 Channel 关闭，实现**优雅停机**。
* **异常恢复:** 自动从 Worker 的 panic 中恢复，防止流水线因异常而死锁。
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

定义你的任务结构体和处理函数。无需实现任何接口。

```go
package main

import (
    "context"
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

// 工序 A: 并发 3
func processA(task MyTask) (MyTask, error) {
    fmt.Printf("A Worker: 正在处理任务 %d\n", task.ID)
    time.Sleep(time.Millisecond * 100)
    task.DataA = fmt.Sprintf("任务 %d 已被 A 处理", task.ID)
    return task, nil
}

// 工序 B: 并发 4 (消费 A 的输出)
func processB(task MyTask) (MyTask, error) {
    fmt.Printf("B Worker: 正在处理任务 %d (DataA: %s)\n", task.ID, task.DataA)
    time.Sleep(time.Millisecond * 150)
    task.DataB = fmt.Sprintf("任务 %d 已被 B 处理", task.ID)
    return task, nil
}

// 工序 C: 并发 5 (最终工序，消费 B 的输出)
func processC(task MyTask) (MyTask, error) {
    fmt.Printf("C Worker: 正在进行最终处理任务 %d (DataB: %s)\n", task.ID, task.DataB)
    time.Sleep(time.Millisecond * 50)
    return task, nil
}
```

#### 2\. 构建和运行流水线

```go
func main() {
    const totalTasks = 20
    ctx := context.Background()
    
    // 1. 创建 Pipeline 实例，指定泛型类型和 Channel 缓冲区大小
    pipe := gopipe.NewPipeline[MyTask](totalTasks)

    // 2. 添加工序 (A: 3 worker, B: 4 worker, C: 5 worker)
    pipe.AddStage("StageA", 3, processA).
        AddStage("StageB", 4, processB).
        AddStage("StageC", 5, processC) 

    // 3. 运行并获取输入/输出 Channel
    inputCh, outputCh := pipe.Run(ctx)

    // 4. 生产任务
    for i := 1; i <= totalTasks; i++ {
        inputCh <- MyTask{ID: i}
    }

    // 5. 关闭输入 Channel，以信号通知流水线开始关闭
    close(inputCh)

    // 6. 消费最终输出
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