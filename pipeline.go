package gopipe

import (
	"sync"
)

// Task is the interface that tasks in the pipeline must implement.
// For generality, a task must provide a unique ID.
type Task interface {
	GetID() int
}

// StageFunc defines the processing logic for each stage.
// It receives a task from the upstream, processes it, and returns it for the next stage.
// If the returned error is not nil, the task's subsequent delivery in the pipeline will be stopped.
type StageFunc func(task Task) (Task, error)

// Stage defines the configuration information for a pipeline stage.
type Stage struct {
	Name    string
	Workers int
	Process StageFunc
}

// Pipeline struct is the core of the entire utility.
type Pipeline struct {
	stages []Stage
	wg     sync.WaitGroup
	buffer int // Channel buffer size
}

// NewPipeline creates a new Pipeline instance.
func NewPipeline(bufferSize int) *Pipeline {
	return &Pipeline{
		stages: make([]Stage, 0),
		buffer: bufferSize,
	}
}

// AddStage adds a stage to the Pipeline.
func (p *Pipeline) AddStage(name string, workers int, fn StageFunc) *Pipeline {
	if workers <= 0 {
		workers = 1 // Ensure at least one worker
	}
	p.stages = append(p.stages, Stage{
		Name:    name,
		Workers: workers,
		Process: fn,
	})
	return p
}

// Run starts the entire Pipeline and begins processing tasks.
// It returns an input Channel and an output Channel.
func (p *Pipeline) Run() (chan<- Task, <-chan Task) {
	// 1. Create the initial input and final output Channels
	input := make(chan Task, p.buffer)
	output := make(chan Task, p.buffer)

	// 2. Initialize the Channel chain: the first input Channel is 'input'
	inCh := input

	// 3. Iterate and start all stages
	for i, stage := range p.stages {
		var outCh chan Task
		// If it's the last stage, the output connects to the final 'output' Channel
		if i == len(p.stages)-1 {
			outCh = output
		} else {
			// Otherwise, create a new Channel to connect to the next stage
			outCh = make(chan Task, p.buffer)
		}

		// Start the stage Goroutine (Worker Pool)
		p.startStage(stage, inCh, outCh)

		// Update the input Channel to be the current stage's output Channel
		inCh = outCh
	}

	return input, output
}

// startStage starts the Worker Pool for a single stage.
// It handles worker counting and Channel closing logic.
func (p *Pipeline) startStage(stage Stage, inCh <-chan Task, outCh chan Task) {
	p.wg.Add(stage.Workers) // Increase Worker count
	var exitCounter sync.Mutex
	exitedWorkers := 0

	for i := 0; i < stage.Workers; i++ {
		go func(workerID int) {
			defer p.wg.Done() // Decrease count when Worker exits

			for task := range inCh {
				// Process the task
				processedTask, err := stage.Process(task)
				if err != nil {
					// Error handling: logging can be done here, task is not passed downstream
					continue
				}

				// Pass to the downstream stage upon success
				if processedTask != nil {
					outCh <- processedTask
				}
			}

			// Close the output Channel when all Workers exit
			// This implements the cascading shutdown logic: when a stage finishes, it closes the next stage's input.
			exitCounter.Lock()
			exitedWorkers++
			if exitedWorkers == stage.Workers && outCh != nil {
				close(outCh)
			}
			exitCounter.Unlock()
		}(i + 1)
	}
}

// Wait blocks until all Workers in all stages have completed their tasks and exited.
func (p *Pipeline) Wait() {
	p.wg.Wait()
}
