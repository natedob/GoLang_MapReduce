package mr

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"regexp"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	lock          sync.Mutex
	mappingTasks  map[int]Task
	reducingTasks map[int]Task
	NReduceTasks  int
	MappingDone   bool
	ReducingDone  bool
	S3Connection  *s3.S3
	BucketName    string
}

type Task struct {
	id          int
	FileName    string
	ReduceFiles []string
	finished    bool
	working     bool
}

func (c *Coordinator) SetBucketName() {
	fmt.Printf("Insert Bucketname: ")

	_, err := fmt.Scanln(&c.BucketName)

	if CheckError(err, "During Scanln bucketName") {
		return
	}
}

/*
WorkHandler handles the call from the worker processes.
It first checks if the worker has requested a task and then checks
if all mapping and reducing tasks are finnished, if they are not finnished
then it assignes the worker to an apporpriate task.
A reducetask can only be assigned when all mapping tasks is done.
If the call is an answer then it checks wether it was a mapping or reduce task
If mapping , it checks the format and distributes the tempfile to the reducingTasks map,
and marks the mapping-task as done. If reducing, it marks the reducing-task as finished.
*/

func (c *Coordinator) WorkerHandler(WorkerArgs *WorkerArgs, CordReply *CoordinatorReply) error {

	if WorkerArgs.Request {

		if !c.CheckMappingFinished() { //Check if all mapping tasks is finished

			c.AssignMappingTask(CordReply)

		} else if !c.CheckReducingFinished() { //Check if all reducingtasks is finished
			c.AssignReducingTask(CordReply)

		} else { //No more work to be done, Shut down the worker.
			CordReply.ShutdownWorker = true
		}

	}
	if WorkerArgs.Answer {

		if WorkerArgs.MappingTask {

			for i := 0; i < c.NReduceTasks; i++ {
				newReduceFiles := []string{}
				for _, fileName := range WorkerArgs.IntermediateFiles {

					expectedFormat := regexp.MustCompile(fmt.Sprintf(`^mr-%d-%d$`, WorkerArgs.TaskId, i))
					if expectedFormat.MatchString(fileName) {
						newReduceFiles = append(newReduceFiles, fileName)
					}
				}
				c.lock.Lock()
				task := c.reducingTasks[i]
				task.ReduceFiles = append(task.ReduceFiles, newReduceFiles...) //"..." används i Go för att packa upp en slice (eller array) och skicka dess element som separata argument till en funktion eller lägga till dem i en annan slice.
				c.reducingTasks[i] = task
				c.lock.Unlock()
			}
			//Set the mapping task to Finished
			c.lock.Lock()
			task := c.mappingTasks[WorkerArgs.TaskId]
			task.finished = true
			c.mappingTasks[WorkerArgs.TaskId] = task
			c.lock.Unlock()

		} else if WorkerArgs.ReduceTask {
			var task2 Task
			c.lock.Lock()
			task2 = c.reducingTasks[WorkerArgs.TaskId]
			task2.finished = true
			c.reducingTasks[WorkerArgs.TaskId] = task2
			c.lock.Unlock()
		}
	}
	return nil
}

/*
AssignMappingTask assigns a mapping-task to a calling worker. Checks if there is any
free task to give. If there is, it creates and sends a CordReply containing the task and additional info including the bucket name
then starts a seperate GO-rutine CheckStrugglres to follow each task given. If no task is free,
we check if all the task is finished with CheckMappingFinished, if all is not finished we return MappingTask and
ReduceTask = false, to make the calling worker wait and call again.
*/

func (c *Coordinator) AssignMappingTask(CordReply *CoordinatorReply) {

	TaskToAssign, ok, index := c.returnFreeTask("Mapping")
	if ok {
		c.lock.Lock()

		TaskToAssign.working = true
		CordReply.Task = TaskToAssign //Set the CordReply.FilesToWorkOn = a task in mappingTasks
		CordReply.NReduceTask = c.NReduceTasks
		CordReply.MappingTask = true
		CordReply.TaskNumber = TaskToAssign.id
		CordReply.BucketName = c.BucketName
		//Write the changed task back into Cord-struct
		c.mappingTasks[index] = TaskToAssign

		go c.CheckStrugglres(true, index)
		c.lock.Unlock()
	} else {
		println("Coordinator: No free Mapping Tasks")

		if c.CheckMappingFinished() {
			println("Coordinator: All Mapping tasks is finished")

		} else { //Not every task is Finished, tell the worker to wait.
			println("Coordinator: Sending Wait to Worker")
			c.lock.Lock()
			CordReply.MappingTask = false
			CordReply.ReduceTask = false
			c.lock.Unlock()
		}
	}
}

/*
AssignReducingTask assigns a reducing-task to a calling worker. Checks if there is any
free task to give. If there is, it creates and sends a CordReply containing the task and additional info,
then starts a seperate GO-rutine CheckStrugglres to follow each task given. If no task is free,
we check if all the task is finished with CheckReducingFinished, if all is not finished we return MappingTask and
ReduceTask = false, to make the calling worker wait and call again.
*/

func (c *Coordinator) AssignReducingTask(CordReply *CoordinatorReply) {

	TaskToAssign, ok, index := c.returnFreeTask("Reducing")
	if ok {
		c.lock.Lock()
		TaskToAssign.working = true
		CordReply.Task = TaskToAssign //Set the CordReply.FilesToWorkOn = a task in ReducingTasks
		CordReply.NReduceTask = c.NReduceTasks
		CordReply.ReduceTask = true
		CordReply.TaskNumber = TaskToAssign.id
		CordReply.BucketName = c.BucketName
		//Write the changed task back into Cord-struct
		c.reducingTasks[index] = TaskToAssign

		go c.CheckStrugglres(false, index)

		c.lock.Unlock()
	} else {
		println("Coordinator: No free Reducing Tasks")

		if c.CheckReducingFinished() {
			println("Coordinator: All Reducing tasks is finished")

		} else { //Not every task is Finished, tell the worker to wait.
			println("Coordinator: Sending Wait to Worker")
			c.lock.Lock()
			CordReply.MappingTask = false
			CordReply.ReduceTask = false
			c.lock.Unlock()
		}
	}
}

/*
CheckStrugglers waits 10 seconds after a worker has been assigned a task, then
it checks if the assigned task has been finnished by the worker
if its not finnished then the CheckStrugglers reassignes the task
*/

func (c *Coordinator) CheckStrugglres(Mapping bool, index int) {
	time.Sleep(10 * time.Second)

	var currentTask Task

	c.lock.Lock()
	if Mapping {

		currentTask = c.mappingTasks[index]
	} else {
		currentTask = c.reducingTasks[index]
	}
	c.lock.Unlock()

	if currentTask.finished {
		return
	}

	c.lock.Lock()
	if Mapping {
		if currentTask.working {

			currentTask.working = false
			c.mappingTasks[index] = currentTask
		}
	} else {
		if currentTask.working {
			currentTask.working = false
			c.reducingTasks[index] = currentTask
		}
	}
	c.lock.Unlock()
}

/*
Checks if the variable c.ReducingDone is true, returns true if it´s true,
else it loops through all reducingTask checking if they are finished. If all is
finished, sets c.ReducingDone = true, and returns true, else it returns false.
*/

func (c *Coordinator) CheckReducingFinished() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.ReducingDone == true {
		return true
	} else {
		for _, task := range c.reducingTasks { //[task1,task2,task3.....]
			if !task.finished {
				return false
			}
		}
		c.ReducingDone = true
		return true
	}
}

/*
Checks if the variable c.MappingDone is true, returns true if it´s true,
else it loops through all mappingTask checking if they are finished. If all is
finished, sets c.MappingDone = true, and returns true, else it returns false.
*/

func (c *Coordinator) CheckMappingFinished() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.MappingDone == true {
		return true
	} else {
		for _, task := range c.mappingTasks { //[task1,task2,task3.....]
			if !task.finished {
				return false
			}
		}
		c.MappingDone = true
		return true
	}
}

/*
start a thread that listens for RPCs from worker.go
*/

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	CheckError(e, "During net.Listen in Coordinator")
	go http.Serve(l, nil)
}

/*
main/mrcoordinator.go calls Done() periodically to find out
if the entire job has finished.
*/

func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.MappingDone && c.ReducingDone //Only needs to check ReducingDOne, since reduce cannot start until apping is Done
}

/*
create a Coordinator.
main/mrcoordinator.go calls this function.
nReduce is the number of reduce tasks to use.
During MakeCoordinator a S3 connection is create with connectToS3 function.
The Coordinator structs is created.
The files []string is filled with the files on the S3 bucket with name "c.BucketName"
The filenames is putted into individual Task structs and putted into c.mappingTasks.
Task with only id numbers is putted into c.reducingTasks for future use.
*/

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Printf("Coordinator Started \n")

	S3connection := ConnectToS3()

	c := Coordinator{
		mappingTasks:  make(map[int]Task),
		reducingTasks: make(map[int]Task),
		NReduceTasks:  nReduce,
		MappingDone:   false,
		ReducingDone:  false,
		S3Connection:  S3connection,
	}

	c.SetBucketName()

	files = listAllFromS3(c.BucketName, c.S3Connection)

	fmt.Printf("Fetched files from S3 - %s,\n", c.BucketName)
	for _, file := range files {
		fmt.Println(file)
	}

	for i, file := range files {
		c.mappingTasks[i] = Task{id: i, FileName: file, finished: false, working: false}
	}

	for i := 0; i < nReduce; i++ {
		c.reducingTasks[i] = Task{id: i, finished: false, working: false}
	}

	c.server()
	return &c
}

/*
Check and return an available task of a specified type Mapping/Reducing from coordinator task lists.
Using Lock to be sure about thread safety during the searching.
If the task is not working and not finished, that means that the task is free
return the free task with boolean indicating its availability and the index for the task in the list.
*/

func (c *Coordinator) returnFreeTask(kindofTask string) (Task, bool, int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if kindofTask == "Mapping" {
		for index, task := range c.mappingTasks { //[task1,task2,task3.....]
			if !task.finished && !task.working {
				return task, true, index
			}
		}
		//no free mapping task available, return empty Task and false
	} else if kindofTask == "Reducing" {
		for index, task := range c.reducingTasks { //[task1,task2,task3.....]
			if !task.finished && !task.working {
				return task, true, index
			}
		}
	}

	return Task{}, false, -1
}

/*
Establish a connection to an AWSs S3 in specified region.
Creates a new session with AWS using configuration options.
Sets up the S3 using the session, and returns the S3 client.
*/
func ConnectToS3() *s3.S3 {

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	})
	CheckError(err, "while connecting (session.NewSessionWithOptions) to S3")

	s3Svc := s3.New(sess)

	fmt.Println("Session created, connected to S3")

	return s3Svc
}
