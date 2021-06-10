package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// define the task type
type TaskType int32
const (
	ToMap	TaskType = 1
	ToReduce	TaskType = 2
	Wait		TaskType = 3
	Done		TaskType = 4
)

// Map task
type MapTask struct {
	FileNmae 	string
}

// Reduce task
type ReduceTask struct {
	Partition 	[]KeyValue
	Index 		int

}

// define the message in rpc req
// ask coordiantor
type ArgsToTask struct {

}

type TaskForReply struct {
	TaskType 	TaskType
	MapTasks 	*MapTask
	ReduceTasks *ReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
