package mapreduce

import (
	"fmt"
	"main/gossip"
	"main/sdfs"
)

func handleMapResultWorker(request MapReduceMsg, myID int64, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, jobFinishchan chan int) {
	fmt.Printf("Map job execution time: %v\n", request.Duration)
	fmt.Println("------------------------------------------------------------")
	jobFinishchan <- request.JobID
}

func handleQueryResultWorker(request MapReduceMsg, myID int64, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, jobFinishchan chan int) {
	resultFileName := request.FileName
	fmt.Printf("Query job execution time: %v\n", request.Duration)
	sdfs.Get(resultFileName, resultFileName, fileTable, 0, membershipMap)
	fmt.Printf("Result %s retrieved from SDFS\n", resultFileName)
	fmt.Println("------------------------------------------------------------")
	jobFinishchan <- request.JobID
}

func handleReduceResultWorker(request MapReduceMsg, myID int64, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, jobFinishchan chan int) {
	fmt.Printf("Reduce job execution time: %v\n", request.Duration)
	fmt.Println("------------------------------------------------------------")
	jobFinishchan <- request.JobID
}

func handleJoinResultWorker(request MapReduceMsg, myID int64, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, jobFinishchan chan int) {
	resultFileName := request.FileName
	fmt.Printf("Join job execution time: %v\n", request.Duration)
	sdfs.Get(resultFileName, resultFileName, fileTable, 0, membershipMap)
	fmt.Printf("Result %s retrieved from SDFS\n", resultFileName)
	fmt.Println("------------------------------------------------------------")
}
