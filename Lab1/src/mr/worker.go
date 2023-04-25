package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		filename, ClientId, JobType, ClientSum, HashNow := RequireAJob()
		if JobType == 0 {
			DoingMap(filename, ClientId, mapf)
			//fmt.Println("Done the map")
		} else if JobType == 1 {
			DoingReduce(ClientSum, HashNow, ClientId, reducef)
			//fmt.Println("Done the reduce")
		}
	}
}

func DoingReduce(ClientSum int, hashNow int, ClientId int, reducef func(string, []string) string) {
	fmt.Printf("Doing a reduce job, client id = %d, hashNow = %d, clientsum = %d\n", ClientId, hashNow, ClientSum)
	i := 1
	mainName := "mr-out-" + strconv.Itoa(ClientId)
	mainFile, _ := os.Create(mainName)
	defer func(mainFile *os.File) {
		err := mainFile.Close()
		if err != nil {
			return
		}
	}(mainFile)
	var kva []KeyValue
	//fmt.Printf("ClientSum = %d\n", ClientSum)
	for i <= ClientSum {
		//fmt.Println(i)
		OName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(hashNow)
		OFile, err := os.OpenFile(OName, os.O_RDONLY, 0644)
		if err != nil {
			i++
			if hashNow == 1464321167 {
				fmt.Println("EEEEEEEEE")
			}
			continue
		}
		//fmt.Printf("Read %s\n", OName)
		dec := json.NewDecoder(OFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//fmt.Printf("Failed to decode file: %s\n", OName)
				break
			}
			kva = append(kva, kv)
		}
		i++
		err = OFile.Close()
		if err != nil {
			fmt.Printf("Failed to close file: %s\n", OName)
			return
		}
	}
	//fmt.Println("Reduce: Done Read")
	sort.Sort(ByKey(kva))
	l := 0
	for l < len(kva) {
		j := l + 1
		for j < len(kva) && kva[j].Key == kva[l].Key {
			j++
		}
		var values []string
		for k := l; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[l].Key, values)
		fmt.Fprintf(mainFile, "%v %v\n", kva[l].Key, output)
		l = j
	}
	TellFinish(ClientId, make(map[int]bool), 1, hashNow)
}

func DoingMap(filename string, ClientId int, mapf func(string, string) []KeyValue) {
	fmt.Printf("Doing a map job, client id = %d, filename = %s\n", ClientId, filename)
	if filename == "error" || ClientId == 0 {
		return
	}
	var HanhNowList = make(map[int]bool)
	var kva = DealSingleFile(filename, mapf)
	i := 0
	for i < len(kva) {
		hashNow := ihash(kva[i].Key)
		HanhNowList[hashNow] = true
		oname := "mr-" + strconv.Itoa(ClientId) + "-" + strconv.Itoa(hashNow)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		kvv := KeyValue{kva[i].Key, kva[i].Value}
		enc := json.NewEncoder(ofile)
		enc.Encode(&kvv)
		ofile.Close()
		i++
	}
	TellFinish(ClientId, HanhNowList, 0, 0)
}

func TellFinish(ClientId int, list map[int]bool, JobType int, HashNow int) bool {
	args := TellFinishArgs{ClientId: ClientId, HashNowList: list, JobType: JobType, HashNow: HashNow}
	reply := TellFinishReply{}
	ok := call("Coordinator.TellFinish", &args, &reply)
	if ok {
		//fmt.Printf("Client id = %d ok!\n", ClientId)
		return true
	} else {
		fmt.Println("Error, can tell finish!")
		return false
	}
}

func RequireAJob() (string, int, int, int, int) {
	// Require a job, return a file name

	args := RequireJobArgs{}
	reply := RequireJobReply{}

	ok := call("Coordinator.ReturnJob", &args, &reply)
	if ok {
		//fmt.Println(reply.FileName)
		//fmt.Println("Doing on " + reply.FileName)
		return reply.FileName, reply.ClientId, reply.JobType, reply.ClientSum, reply.HashNow
	} else {
		fmt.Println("Error, no job reply!")
		return "error", 0, 0, 0, 0
	}
}

func DealSingleFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	return kva
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
