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
	mu                sync.Mutex
	fileNameList      []string
	onProceedingList  []int
	ProceededList     []int
	clientIdNext      int
	ClientFileNameMap map[int]string
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
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.FileName = c.fileNameList[0]
	reply.ClientId = c.clientIdNext
	c.onProceedingList = append(c.onProceedingList, c.clientIdNext)
	c.ClientFileNameMap[c.clientIdNext] = c.fileNameList[0]
	c.clientIdNext++
	c.fileNameList = append([]string{}, c.fileNameList[1:]...)
	return nil
}

func (c *Coordinator) TellFinish(args *TellFinishArgs, reply *TellFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	temp := 1
	for idx, value := range c.onProceedingList {
		if value == args.ClientId {
			temp = idx
		}
	}
	c.onProceedingList = append(c.onProceedingList[:temp], c.onProceedingList[temp+1:]...)
	c.ProceededList = append(c.ProceededList, args.ClientId)
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
	// Your code here.

	c.server()
	return &c
}
