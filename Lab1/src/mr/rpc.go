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

type RequireJobArgs struct {
}

type RequireJobReply struct {
	FileName  string
	ClientId  int
	JobType   int
	ClientSum int
	HashNow   int
}

type ReceiveResultArgs struct {
}

type ReceiveResultReply struct {
}

type TellFinishArgs struct {
	JobType     int
	ClientId    int
	HashNowList map[int]bool
}

type TellFinishReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
