package gopipe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// StageFunc defines the processing logic for each stage.
// It receives a task from the upstream, processes it, and returns it for the next stage.
// If the returned error is not nil, the task's subsequent delivery in the pipeline will be stopped.
type StageFunc[T any] func(task T) (T, error)

// Stage defines the configuration information for a pipeline stage.
type Stage[T any] struct {
	Name    string
	Workers int
	Process StageFunc[T]
}

// Pipeline struct is the core of the entire utility.
type Pipeline[T any] struct {
	stages []Stage[T]
	wg     sync.WaitGroup
	buffer int // Channel buffer size
}

// NewPipeline creates a new Pipeline instance.
func NewPipeline[T any](bufferSize int) *Pipeline[T] {
	return &Pipeline[T]{
		stages: make([]Stage[T], 0),
		buffer: bufferSize,
	}
}

// AddStage adds a stage to the Pipeline.
func (p *Pipeline[T]) AddStage(name string, workers int, fn StageFunc[T]) *Pipeline[T] {
	if workers <= 0 {
		workers = 1 // Ensure at least one worker
	}
	p.stages = append(p.stages, Stage[T]{
		Name:    name,
		Workers: workers,
		Process: fn,
	})
	return p
}

// Run starts the entire Pipeline and begins processing tasks.
// It returns an input Channel and an output Channel.
func (p *Pipeline[T]) Run(ctx context.Context) (chan<- T, <-chan T) {
	// 1. Create the initial input and final output Channels
	input := make(chan T, p.buffer)
	output := make(chan T, p.buffer)

	// 2. Initialize the Channel chain: the first input Channel is 'input'
	inCh := input

	// 3. Iterate and start all stages
	for i, stage := range p.stages {
		var outCh chan T
		// If it's the last stage, the output connects to the final 'output' Channel
		if i == len(p.stages)-1 {
			outCh = output
		} else {
			// Otherwise, create a new Channel to connect to the next stage
			outCh = make(chan T, p.buffer)
		}

		// Start the stage Goroutine (Worker Pool)
		p.startStage(ctx, stage, inCh, outCh)

		// Update the input Channel to be the current stage's output Channel
		inCh = outCh
	}

	return input, output
}

// startStage starts the Worker Pool for a single stage.
// It handles worker counting and Channel closing logic.
func (p *Pipeline[T]) startStage(ctx context.Context, stage Stage[T], inCh <-chan T, outCh chan T) {
	p.wg.Add(stage.Workers) // Increase Worker count
	exitedWorkers := int32(0)

	for i := 0; i < stage.Workers; i++ {
		go func() {
			defer p.wg.Done() // Decrease count when Worker exits
			defer func() {
				// Close the output Channel when all Workers exit
				// This implements the cascading shutdown logic: when a stage finishes, it closes the next stage's input.
				if atomic.AddInt32(&exitedWorkers, 1) == int32(stage.Workers) && outCh != nil {
					close(outCh)
				}
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-inCh:
					if !ok {
						return
					}
					// Process the task
					processedTask, err := func() (t T, e error) {
						defer func() {
							if r := recover(); r != nil {
								e = errors.New("panic in stage")
							}
						}()
						return stage.Process(task)
					}()
					if err != nil {
						// Error handling: logging can be done here, task is not passed downstream
						continue
					}

					// Pass to the downstream stage upon success
					select {
					case <-ctx.Done():
						return
					case outCh <- processedTask:
					}
				}
			}
		}()
	}
}

// Wait blocks until all Workers in all stages have completed their tasks and exited.
func (p *Pipeline[T]) Wait() {
	p.wg.Wait()
}
