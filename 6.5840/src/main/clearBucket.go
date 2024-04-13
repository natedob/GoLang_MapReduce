package main

import (
	"6.5840/mr"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	fmt.Print("Enter the S3 bucket name: ")
	var bucketName string
	fmt.Scanln(&bucketName)

	s3Client := mr.ConnectToS3()

	// Specify the prefix for files to delete
	prefixToDelete := "mr-"

	// List objects in the bucket with the specified prefix
	listObjectsInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefixToDelete),
	}

	result, err := s3Client.ListObjects(listObjectsInput)
	if err != nil {
		fmt.Println("Error listing objects in S3 bucket:", err)
		return
	}
	weHappy := true
	// Delete each object
	for _, obj := range result.Contents {
		deleteObjectInput := &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(*obj.Key),
		}

		_, err := s3Client.DeleteObject(deleteObjectInput)
		if err != nil {
			fmt.Printf("Error deleting object %s: %v\n", *obj.Key, err)
			weHappy = false
		}
	}
	if weHappy {
		fmt.Println("Deleted object all object")
	}
}
