package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// this is the sync vars for the local variables
type partitionInfo struct {
	index int 			// partition index
	allLength int
}

type Coordinator struct {
	// Your definitions here.
	files	[]string				//
	fileIndex int					// the index of the files
	intermediatePairs []KeyValue	// the local disk to store the map results
	localLock sync.Mutex			// need a lock to guarantee the thread safe
	parInfo *partitionInfo			// control the access of the reducePartition
	isPartitionInitialized bool
	reducePartition [][]KeyValue		//
	isMapDone bool 					// flag of map all map tasks
	isAllDone bool					// flag on all the tasks
}

// Your code here -- RPC handlers for the worker to call.
// RPC handle for the Work
func (c *Coordinator) DeliverTask(args *ArgsToTask, reply *TaskForReply) error {
	//fmt.Println("Coordinator: Receive the DeliverTask ask ")
	// check tasks have been done
	c.localLock.Lock()
	defer c.localLock.Unlock()
	if !c.isMapDone {
		//fmt.Println("Coordinator: Receive the Map ask ")
		newFileIndex := c.fileIndex


		mapTask := &MapTask{
			FileNmae: c.files[newFileIndex],
		}
		reply.TaskType = ToMap
		reply.MapTasks = mapTask
		return  nil
	}else {
		// make sure the partition only initionalized once
		if !c.isPartitionInitialized {
			reduces := len(c.reducePartition)
			gapLength := len(c.intermediatePairs) /reduces
			for i := 0; i < reduces; i++ {
				if i == reduces -1 {
					c.reducePartition[i]  = c.intermediatePairs[10*i:]
				}else {
					c.reducePartition[i]  = c.intermediatePairs[10*i:10*i+gapLength]
				}
			}
			c.isPartitionInitialized = true
		}

		// check reduce tasks done
		if !c.isAllDone {
			fmt.Println("Coordinator: Receive the Reduce ask ")
			//c.localLock.Unlock()
			newIndex := c.parInfo.index
			c.parInfo.index += 1
			reduceTask := &ReduceTask{
				Partition: c.reducePartition[newIndex],
				Index: newIndex,
			}
			reply.TaskType = ToReduce
			reply.ReduceTasks = reduceTask
			return nil
		}else {
			reply.TaskType = Done
			return nil
		}
	}
	return nil
}

// sync the intermediates
func (c *Coordinator) SyncIntermediate(intermediaPair []KeyValue, isSync *bool) error {
	fmt.Println("Coordinator: Receive the SyncIntermediate ask ")
	c.localLock.Lock()
	c.intermediatePairs = append(c.intermediatePairs, intermediaPair...)
	c.fileIndex += 1
	if c.fileIndex == len(c.files) {
		c.isMapDone = true
	}
	c.localLock.Unlock()
	*isSync = true
	return nil
}

// sycn the partition for futher task
func (c *Coordinator) SyncPartitionIndex(args *ArgsToTask,isOk *bool) error {
	c.localLock.Lock()
	c.parInfo.allLength += 1
	fmt.Printf("allLength: %d --  ", c.parInfo.allLength)
	fmt.Printf("reducePartition: %d \n", len(c.reducePartition))
	if c.parInfo.allLength == len(c.reducePartition) {
		c.isAllDone = true
	}
	c.localLock.Unlock()
	*isOk = true
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.localLock.Lock()
	if c.isAllDone {
		fmt.Println("Ohhhh, all the task has been done")
		ret = c.isAllDone
	}
	c.localLock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.reducePartition = make([][]KeyValue, nReduce)
	c.files = files
	c.intermediatePairs = make([]KeyValue, 0)
	c.isMapDone = false
	c.isAllDone = false
	c.fileIndex = 0
	c.isPartitionInitialized = false
	c.parInfo = &partitionInfo{
		index:       0,
		allLength:   0,
	}
	c.server()
	//fmt.Println("Coordinator: OK, here is the Coordinator working...")
	return &c
}
