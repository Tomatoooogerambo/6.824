package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// implement the Sort interface
// SortKV could sort the slice of the KeyValue in order
type SortKV []KeyValue
// for sorting by key.
func (a SortKV) Len() int           { return len(a) }
func (a SortKV) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKV) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := ArgsToTask{}
		reply := TaskForReply{}
		call("Coordinator.DeliverTask", &args, &reply)
		// checke the task type and  do the work
		switch reply.taskType {
		case ToMap:
			// Map task
			// get the file and map it
			DoMapTask(&reply, mapf)
		case ToReduce:
			// Reduce Task
			DoReduceTask(&reply, reducef)
		}
	}
}

// do the map task
func DoMapTask(reply *TaskForReply, mapTask func(string, string) []KeyValue) {
	fileName := reply.mapTasks.fileNmae
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	fileContent, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file #{fileName}")
	}
	file.Close()
	intermediatePairs := mapTask(fileName, string(fileContent))
	// send the map result to the coordinator
	// return the partition to the coordinator to sync the local storage
	ok := CallForSyncIntermediaMemory(intermediatePairs)
	if !ok {
		log.Fatalf("Sync intermediate failed")
	}
}

// do the reduce task
func DoReduceTask(reply *TaskForReply, reduceTask func(string, []string) string) {
	partition := reply.reduceTasks.partition
	fileIndex := reply.reduceTasks.index
	sort.Sort(SortKV(partition))
	outPut := "mr-out-" + strconv.Itoa(fileIndex)
	outFile, _ := os.Open(outPut)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out- "index" file
	//
	i := 0
	for i < len(partition) {
		j := i + 1
		for j < len(partition) && partition[j].Key == partition[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, partition[k].Value)
		}
		output := reduceTask(partition[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", partition[i].Key, output)

		i = j
	}
	outFile.Close()
	ok :=  CallForSyncPartition()
	if !ok {
		log.Fatalf("Sync intermediate failed")
	}
}

// call the coordinator to sync the intermediate memory
func CallForSyncIntermediaMemory(intermediatePairs []KeyValue) bool {
	isSync := false
	call("Coordinator.SyncIntermediate", intermediatePairs, &isSync)
	return isSync
}

// call the coordinator to sync the index of the partition
func CallForSyncPartition() bool {
	isOk := false
	call("Coordinator. SyncPartitionIndex", &ArgsToTask{},&isOk)
	return isOk
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
