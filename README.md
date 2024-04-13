# Distributed systems Lab2 part2

This part allows to run do map reduce program distributed through multiple machines. 
In our solution we use an AWS S3 bucket as our distributed file system, and the 
coordinator and the workers should each run on separate AWS EC2 instances, where all instances
have read-write access to the bucket.

## Commands

### Dist Lab2:
cd distributable_systems_lab2/6.5840/src/main/

### Dist Lab2-part2
cd distributable_systems_lab2_part2/6.5840/src/main/

### In Main:
go build -buildmode=plugin ../mrapps/wc.go   (Bygg wc)

### In Main Run coordinator:
rm mr-out* 
go run -race mrcoordinator.go pg-*.txt

### In other window, Run worker
go run mrworker.go wc.so

### För att köra med Race:

1:  go build -race -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

2: go run -race mrcoordinator.go pg-*.txt

3: go run -race -gcflags="all=-N -l" mrworker.go wc.so

### if no race test:

go build  -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go

go run -gcflags="all=-N -l" mrworker.go wc.so


### TESTING
bash test-mr.sh

### to clear the bucket
run go clearBucket.go