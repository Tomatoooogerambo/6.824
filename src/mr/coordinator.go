package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	files	[]string				//
	fileIndex int					// the index of the files
	intermediatePairs []KeyValue	// the local disk to store the map results
	localLock sync.Mutex			// need a lock to guarantee the thread safe
	partitionIndex int 				// index to the partition
	reducePartition [][]KeyValue		//
	isMapDone bool 					// flag of map all map tasks
	isAllDone bool					// flag on all the tasks
}

// Your code here -- RPC handlers for the worker to call.
// RPC handle for the Work
func (c *Coordinator) DeliverTask(args *ArgsToTask, reply *TaskForReply) error {

	// check tasks have been done
	if !c.isMapDone {
		c.localLock.Lock()
		newFileIndex := c.fileIndex
		c.localLock.Unlock()

		mapTask := &MapTask{
			fileNmae: c.files[newFileIndex],
		}

		reply.taskType = ToMap
		reply.mapTasks = mapTask
		return  nil
	}else {
		// check reduce tasks done
		if !c.isAllDone {
			c.localLock.Lock()
			partitionIndex := c.partitionIndex
			c.localLock.Unlock()

			reduceTask := &ReduceTask{
				partition: c.reducePartition[partitionIndex],
			}
			reply.taskType = ToReduce
			reply.reduceTasks = reduceTask
			return nil
		}else {
			reply.taskType = Done
			return nil
		}

	}
	return nil
}

// sync the intermediates
func (c *Coordinator) SyncIntermediate(intermediaPair []KeyValue, isSync *bool) error {
	c.localLock.Lock()
	c.intermediatePairs = append(c.intermediatePairs, intermediaPair...)
	c.localLock.Unlock()
	*isSync = true
	return nil
}

// sycn the partition for futher task
func (c *Coordinator) SyncPartitionIndex(index *int, isOk *bool) error {
	c.localLock.Lock()
	c.partitionIndex = *index
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
	if c.isAllDone {
		ret = c.isAllDone
	}

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
	c.intermediatePairs = make([]KeyValue, len(files))
	c.isMapDone = false
	c.isAllDone = false
	c.partitionIndex = 0
	c.fileIndex = 0

	c.server()
	return &c
}
