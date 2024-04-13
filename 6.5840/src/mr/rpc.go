package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type WorkerArgs struct {
	Request           bool
	Answer            bool
	MappingTask       bool
	ReduceTask        bool
	TaskId            int
	IntermediateFiles []string
}

type CoordinatorReply struct {
	TaskNumber     int
	Task           Task
	NReduceTask    int
	ReduceTask     bool
	MappingTask    bool
	ShutdownWorker bool
	BucketName     string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
