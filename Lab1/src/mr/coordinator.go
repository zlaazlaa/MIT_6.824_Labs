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

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	fileNameList      []string
	onProceedingList  []int
	ProceededList     []int
	clientIdNext      int
	ClientFileNameMap map[int]string
	AllResult         []KeyValue
	jobStatus         int // 0 -> map, 1 -> reduce
	HashMapListAll    map[int]bool
	MapClientSum      int
	HashList          []int
	ProceedingHash    []int
	ProceedHash       []int
	ClientSum         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReturnJob(args *RequireJobArgs, reply *RequireJobReply) error {
	//if c.jobStatus == 1 && len(c.ProceededList) != c.ClientSum {
	//	for {
	//		if len(c.ProceededList) == c.ClientSum {
	//			fmt.Println("Waiting for map to complete")
	//			break
	//		}
	//	}
	//
	//}
	c.mu.Lock()
	defer c.mu.Unlock()
	//fmt.Printf("Proceeding hash size = %d\n", len(c.ProceedingHash))
	fmt.Printf("Client id %d start!\n", c.clientIdNext)
	fmt.Printf("JobStatus = %d\n", c.jobStatus)
	fmt.Printf("%d %d\n", c.ClientSum, len(c.ProceededList))
	//time.Sleep(5 * time.Second)
	if c.jobStatus == 0 {
		//fmt.Printf("Statue now: %d\n", c.jobStatus)
		if len(c.fileNameList) == 0 {
			reply.ClientId = 0
			return nil
		}
		reply.FileName = c.fileNameList[0]
		reply.ClientId = c.clientIdNext
		reply.JobType = 0
		c.onProceedingList = append(c.onProceedingList, c.clientIdNext)
		c.ClientFileNameMap[c.clientIdNext] = c.fileNameList[0]
		c.clientIdNext++
		if len(c.fileNameList) == 1 {
			c.fileNameList = []string{}
		} else {
			c.fileNameList = append([]string{}, c.fileNameList[1:]...)
		}
		if len(c.fileNameList) == 0 {
			//c.jobStatus = 1
			// c.MapClientSum is only the sum of map client
			c.MapClientSum = c.clientIdNext
		}
	} else {
		//if c.HashList[0] == 1257227291 {
		//	i := 1
		//	for i < 10 {
		//		fmt.Println("========================================================================================================================")
		//		i++
		//	}
		//	fmt.Println(c.HashList[0])
		//	fmt.Println(c.MapClientSum)
		//	fmt.Println(c.clientIdNext)
		//	time.Sleep(10 * time.Second)
		//}
		//fmt.Printf("Statue now: %d\n", c.jobStatus)
		reply.HashNow = c.HashList[0]
		reply.ClientSum = c.MapClientSum
		reply.ClientId = c.clientIdNext
		reply.JobType = 1
		c.clientIdNext++
		c.ProceedingHash = append(c.ProceedingHash, c.HashList[0])
		//fmt.Printf("len  %d\n", len(c.ProceedingHash))
		c.HashList = c.HashList[1:]

		if reply.HashNow == 1464321167 {
			fmt.Println(c.HashList[0])
			fmt.Println(c.MapClientSum)
			fmt.Println(c.clientIdNext - 1)
			fmt.Println()
			fmt.Println()
		}
	}
	return nil
}

func (c *Coordinator) TellFinish(args *TellFinishArgs, reply *TellFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.JobType == 0 {
		temp := 1
		for idx, value := range c.onProceedingList {
			if value == args.ClientId {
				temp = idx
			}
		}
		c.onProceedingList = append(c.onProceedingList[:temp], c.onProceedingList[temp+1:]...)
		c.ProceededList = append(c.ProceededList, args.ClientId)
		for k, _ := range args.HashNowList {
			if k == 1464321167 {
				fmt.Println("QQQQQQQQq")
			}
			c.HashMapListAll[k] = true
		}
		fmt.Printf("ProceedList size = %d\n", len(c.ProceededList))
		if len(c.ProceededList) == c.ClientSum {
			fmt.Println("All map is done")
			// change map to a slice, convenient to use
			for k, _ := range c.HashMapListAll {
				if k == 1464321167 {
					fmt.Println("WWWWWWWWWWw")
				}
				c.HashList = append(c.HashList, k)
			}
			c.jobStatus = 1
		}
	} else if args.JobType == 1 {
		temp := -1
		for idx, value := range c.ProceedingHash {
			if value == args.HashNow {
				temp = idx
			}
		}
		c.ProceedingHash = append(c.ProceedingHash[:temp], c.ProceedingHash[temp+1:]...)
		c.ProceedHash = append(c.ProceedHash, args.ClientId)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.fileNameList = files
	c.clientIdNext = 1
	c.ClientFileNameMap = map[int]string{}
	c.jobStatus = 0
	c.HashMapListAll = map[int]bool{}
	c.ClientSum = len(files)
	// Your code here.
	fmt.Println("start")

	c.server()
	return &c
}
