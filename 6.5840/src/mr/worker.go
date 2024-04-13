package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"os"
	"sort"
	"time"
)

import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const MaxRun int = 9

var Counter int = 0

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerStruct struct {
	S3Connection *s3.S3
	ipadress     string
}

/*
SetUpWorker creates a connection to a S3 with connectToS3 function
Creates WorkerStruct holding the S3Connection.
*/
func SetUpWorker() *WorkerStruct {
	S3Connection := ConnectToS3()
	w := WorkerStruct{S3Connection: S3Connection}
	w.SetIp()
	return &w
}

func (w *WorkerStruct) SetIp() {
	fmt.Printf("Insert coordinator-IP: ")

	_, err := fmt.Scanln(&w.ipadress)

	if CheckError(err, "During Scanln IP-adress") {
		return
	}
}

// main/mrworker.go calls this function.
// Worker calls setUpWorker and then LoopWorkerCall.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := SetUpWorker()
	w.LoopWorkerCall(mapf, reducef)
}

/*
LoopWorkerCall. Makes a call to the coordinators workhandler function. Sends argument Request = true:
Receives reply struct from coorinator (Cordreply). If Cordreply indicates mapping-task mapFunc is started.
If Cordreply indicates reducing task reduceFunc is started. If not a Task of any kind the function checks if CordReply
contains ShutdownWorker = true. If true, exit, if not true wait for 500 ms, then loop and call the coordinator again.
*/
func (w *WorkerStruct) LoopWorkerCall(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	WorkerArgs := WorkerArgs{Request: true}
	CordReply := CoordinatorReply{}

	ok := w.call("Coordinator.WorkerHandler", &WorkerArgs, &CordReply)
	if ok {

		if CordReply.MappingTask {
			w.mapFunc(mapf, CordReply)

		} else if CordReply.ReduceTask {
			w.reduceFunc(reducef, CordReply)

		} else if CordReply.ShutdownWorker != true { //NOT mappingTask, NOT reducingTask: Not shutingDown We wait and call again later
			time.Sleep(200 * time.Millisecond)
			w.LoopWorkerCall(mapf, reducef)

		} else if CordReply.ShutdownWorker == true {
			fmt.Printf("Worker Shuting down on commmand from coordinator!\n")
			//cleanupFiles()
			os.Exit(1) //End with exitcode 1.
		}

	} else { //If call failed
		fmt.Printf("Worker: Call failed!\n")
		os.Exit(1) //End with exitcode 1.
	}

	if Counter <= MaxRun {
		Counter++
		w.LoopWorkerCall(mapf, reducef)
	} else {
		//cleanupFiles()
		fmt.Printf("This worker is Done with %d jobs\n", Counter)
	}
}

/*
Process the mapping task received from the coordinator and read the specified file on the s3-bucket given by the coordinator.
Sort and create map containing key-value pairs and distribute using the CordReply.NReduceTask in modulus function.
Creates temporary files (localy) and on the s3 bucket for each task and sends the temporary file names back to coordinator.
*/

func (w *WorkerStruct) mapFunc(mapf func(string, string) []KeyValue, CordReply CoordinatorReply) {

	filename := CordReply.Task.FileName

	fmt.Printf("Worker Starting mapping Task(%d) on file %s\n", CordReply.TaskNumber, filename)

	filefroms3, err := w.readFromS3(CordReply.BucketName, filename)
	if CheckError(err, "During readfromS3") {
		return
	}

	content_s3 := string(filefroms3) //Convert the []byte string to a string

	ContentArray := mapf(filename, content_s3) //Create a ContentArray = ([KeyValue],[KeyValue])     keyValue = ["word","1"] using the mapf function

	// Sorting the slice 'ContentArray' of KeyValue structs using sort.Slice function
	sort.Slice(ContentArray, func(i, j int) bool {
		// Comparison function for sorting elements at indices 'i' and 'j' of 'ContentArray'
		return ContentArray[i].Key < ContentArray[j].Key // Comparison based on the Key field of KeyValue struct
	})

	//Creating a map "ReduceMap" containing [Key = modnumber, value = keyValue = ["word","1"] ], modnumber = 0 <-> 9
	ReduceMap := make(map[int][]KeyValue)

	for _, value := range ContentArray {
		modnumber := ihash(value.Key) % CordReply.NReduceTask
		ReduceMap[modnumber] = append(ReduceMap[modnumber], value) //  Map [Key = mobnumber, value = keyValue], mobnumber = 0 <-> 9
	}

	//Creating Temp files with name mr-X-Y, where X is the Map-task number, and Y is the reduce task number. (modnumber)
	IntermediateFiles := []string{}

	var buffer bytes.Buffer

	for modnumber, value := range ReduceMap {
		TempFileName := fmt.Sprintf("mr-%v-%v", CordReply.TaskNumber, modnumber) // Ex-mr-0-0, mr-0-1 ..

		enc := json.NewEncoder(&buffer)
		for _, kv := range value {
			err := enc.Encode(&kv)
			if CheckError(err, "during Encode Keyvalue") {
				return
			}
		}

		IntermediateFiles = append(IntermediateFiles, TempFileName) //["mr-0-0","mr-0-1,"]

		// Upload the buffer directly to S3
		err = w.writeToS3(CordReply.BucketName, TempFileName, buffer.Bytes())
		if CheckError(err, "During writeToS3") {
			return
		}
		buffer.Reset() // Reset the buffer for the next iteration

	}
	//Call back to coordinator to inform mapping-Done.
	WorkerArgsDone := WorkerArgs{TaskId: CordReply.TaskNumber, MappingTask: true, IntermediateFiles: IntermediateFiles, Answer: true}
	ok := w.call("Coordinator.WorkerHandler", &WorkerArgsDone, nil)

	if ok {
		println("Worker Done mapping")
	} else {
		println("Error during doneCall")
	}

}

/*
reduceFunc takes care of the reducing part of the program, it receives tempfile-names from coordinator,
and reads the temporary files previously created by a Worker running mapFunc from the s3 bucket given by the coordinator
and runs the reducef function on the files, and then creates output files containing the reduced maps and sends it to the s3 bucket.
*/

func (w *WorkerStruct) reduceFunc(reducef func(string, []string) string, CordReply CoordinatorReply) {
	fmt.Printf("Worker Starting reducing Task(%d) on files\n", CordReply.TaskNumber)

	kva := []KeyValue{} //Array of keyValues [["word","1"]["word2","1"]]
	for _, fileName := range CordReply.Task.ReduceFiles {
		print(fileName + ", ")
		fileFromS3, err := w.readFromS3(CordReply.BucketName, fileName)
		if CheckError(err, "During readfromS3 in reduceFunc") {
			return
		}
		
		//Create a *bytes.reader of the file to be able to decode it from JSON format
		reader := bytes.NewReader(fileFromS3)

		dec := json.NewDecoder(reader)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	//Do the sort after Letter-order
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	OutFileName := fmt.Sprintf("mr-out-%v", CordReply.TaskNumber)

	var buffer bytes.Buffer

	// Perform the reduce operation and write to the buffer
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// Write the result directly to the buffer
		fmt.Fprintf(&buffer, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// Upload the data from the buffer to S3 directly )
	err := w.writeToS3(CordReply.BucketName, OutFileName, buffer.Bytes())
	if CheckError(err, "During writeToS3 in reduceFunc") {
		return
	}

	WorkerArgsDone := WorkerArgs{ReduceTask: true, Answer: true, TaskId: CordReply.TaskNumber}
	ok := w.call("Coordinator.WorkerHandler", &WorkerArgsDone, nil)
	if ok {
		println("Worker Done Reducing")
	} else {
		println("Error during doneCall (Reducing)")
	}
}

/*
send an RPC request to the coordinator, wait for the response.
usually returns true.
returns false if something goes wrong.
*/
func (w *WorkerStruct) call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", w.ipadress+":1234") //The ip-adress should match the EC2 running the coordinator PUBLIC-IP
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	CheckError(err, "During rpc.DialHTTP in worker")
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

/*
CheckError checks if an error has occurred and prints an error message.
It takes an error and a location description as arguments.
If the error is != nil, it indicates an error occurred, then it
prints an error-message with an explanation and returns true.
If error = nil, it returns false to indicate no error.
*/
func CheckError(error error, place string) bool {
	if error != nil {
		fmt.Printf("\x1b[31mError during:%s.\x1b[0m \n", place)
		fmt.Printf("\x1b[31mexplanation Err =:%s.\x1b[0m \n", error)
		return true
	} else {
		return false
	}
}

/*
lists all the files present in the bucket bucketName
*/
func listAllFromS3(bucketName string, s3Svc *s3.S3) []string {
	var fileNames []string

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}

	result, err := s3Svc.ListObjects(input)
	CheckError(err, "Error during ListObjects(Input)")

	//Append to fileNames
	for _, item := range result.Contents {
		fileNames = append(fileNames, *item.Key)
	}

	return fileNames
}

/*
Retrieves the content of the specific file from the S3 bucket.
It takes the bucket's name and file's name as arguments.
Creates an S3 GetObjectInput to request the file's content from the S3 bucket.
It reads the content from the S3 response.
Closes the response to release resources and returns the file's data.
*/
func (w *WorkerStruct) readFromS3(bucketName, FileToRead string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(FileToRead),
	}

	result, err := w.S3Connection.GetObject(input)
	if CheckError(err, "In s3svc.GetObject(input)") {
		{
			return nil, err
		}
	}

	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if CheckError(err, "In io.ReadAll(result.Body)") {
		return nil, err
	}
	return data, nil
}

/*
uploads the file fileName to the s3-bucket bucketName
*/
func (w *WorkerStruct) writeToS3(bucketName, fileName string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader(data),
	}

	_, err := w.S3Connection.PutObject(input)
	return err
}
