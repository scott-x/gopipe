### gopipe: 通用 Go 并发流水线工具 🚀

`gopipe` 是一个轻量级、通用的 Go 语言库，用于构建高并发、可扩展的**流水线（Pipeline）**。它基于 \*\*Worker Pool（协程池）\*\*和 \*\*Channel（通道）\*\*模式，让你能够轻松定义多级处理流程，并为每个工序配置固定的并发度限制。

### ✨ 特性

  * **可配置并发度:** 为每个工序（Stage）设置精确的 `Worker` 数量（例如，工序 A：3 个 Worker，工序 B：4 个 Worker）。
  * **工序解耦:** 各工序通过带缓冲区的 Go Channel 进行异步通信，最大程度减少阻塞。
  * **通用任务处理:** 使用 `Task` 接口实现，具备最大的灵活性，任何结构体都可以作为任务。
  * **自动化清理:** 自动管理 `sync.WaitGroup` 和级联的 Channel 关闭，实现优雅停机。
  * **链式 API:** 使用 Builder 模式 (`AddStage().AddStage()`)，定义流水线流程直观简洁。

### 📦 安装

```bash
go get github.com/scott-x/gopipe
```

### 🚀 使用示例

#### 1\. 定义你的任务

你的任务结构体必须实现 `gopipe.Task` 接口。

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
    myTask := task.(MyTask) // 类型断言
    fmt.Printf("A Worker: 正在处理任务 %d\n", myTask.ID)
    time.Sleep(time.Millisecond * 100)
    myTask.DataA = fmt.Sprintf("任务 %d 已被 A 处理", myTask.ID)
    return myTask, nil
}
```

#### 2\. 构建和运行流水线

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

    // 4. 生产任务 (Input)
    for i := 1; i <= totalTasks; i++ {
        inputCh <- MyTask{ID: i}
    }

    // 5. 关键步骤: 关闭输入 Channel，以信号通知流水线开始关闭
    close(inputCh)

    // 6. 等待所有 Worker 完成
    pipe.Wait()
    fmt.Println("所有流水线任务已完成。")
}
```