package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup // Using Wait Group to sync ongoing tasks
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(taskNum int, nios int, phase jobPhase) {
			debug("DEBUG: current taskNum: %v, nios: %v, phase: %v\n", taskNum, nios, phase)
			defer wg.Done()
			for {
				worker := <-registerChan // get the worker from register Channel
				fmt.Printf("get worker %v \n", worker)
				debug("DEBUG: current worker port: %v\n", worker)

				var args DoTaskArgs
				args.JobName = jobName
				args.File = mapFiles[taskNum]
				args.Phase = phase
				args.TaskNumber = taskNum
				args.NumOtherPhase = nios
				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				go func() {
					registerChan <- worker // it's done
					fmt.Printf("worker %v back to channel \n", worker)
				}()
				if ok {
					fmt.Printf("return is ok\n")
					break
				}
				// if the response is not ok means job is failed we need rerun it
				// worker has been back into channel we will get one from it
			}
		}(i, n_other, phase)
	}
	wg.Wait() // 等待所有的任务完成

	fmt.Printf("Schedule: %v phase done\n", phase)
}
