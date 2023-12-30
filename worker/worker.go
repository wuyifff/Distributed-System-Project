package main

import (
	"bufio"
	"fmt"
	"log"
	"main/consts"
	"main/gossip"
	"main/mapreduce"
	"main/sdfs"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	// Get server address
	args := os.Args
	if len(args) != 2 {
		log.Println("Usage: go run server.go <serverAddr>")
		return
	}
	myAddr := args[1]
	var myID int64

	file, err := os.OpenFile("logs/"+myAddr+".log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	myHostName := strings.Split(myAddr, ":")[0]

	if err != nil {
		log.Fatal(err)
		return
	}
	log.SetFlags(log.Lmicroseconds)
	log.SetOutput(file)

	membershipMap := make(gossip.MembershipMap)
	fileTable := make(sdfs.FileTable)
	localFileTable := make(sdfs.LocalFileTable)

	go receiveListUpdate(myHostName, &fileTable)

	// connect to leader to get current active members
	conn, err := initMemberMap(&membershipMap, myAddr, consts.LEADER_ADDR)
	if err != nil {
		log.Println("Error connecting leader ", err)
		return
	}

	for _, member := range membershipMap {
		if member.Addr == myAddr {
			myID = member.ID
		}
	}

	sus := consts.DEFALUT_SUS
	dropRate := consts.DEFAULT_DROP_RATE
	// wait for signal to command to GOSSIP+S mode
	go ListenCommand(myAddr, myID, &membershipMap, &fileTable, &localFileTable)

	// start send and receive gossip
	var wg sync.WaitGroup
	wg.Add(1)
	var mutex sync.Mutex
	go sendGossip(&membershipMap, myID, &mutex, &sus, &dropRate)
	go receiveGossip(&membershipMap, myID, &mutex, conn, &sus)
	go gossip.PrintMembershipList(&membershipMap, &mutex)
	go sdfs.RequestHandler(myAddr, myID, &localFileTable)
	jobFinishchan := make(chan int)
	go mapreduce.HandlerMapReduceWorker(myHostName, &fileTable, &mutex, &membershipMap, myID, jobFinishchan)

	// _, err = os.Stat(consts.FILE_STORE_PATH + "/" + strconv.FormatInt(myID, 10))
	// if err != nil {
	// 	if !os.IsNotExist(err) {
	// 		fmt.Println("directory error:", err)
	// 		return
	// 	}
	// } else {
	// 	err = os.Remove(consts.FILE_STORE_PATH + "/" + strconv.FormatInt(myID, 10))
	// 	if err != nil {
	// 		fmt.Println("error removing file,  ", err)
	// 		return
	// 	}
	// }

	// Read user commands
	index := 0
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := scanner.Text()
		if command == "" {
			continue
		}
		fields := strings.Fields(command)
		switch fields[0] {
		case "list_mem":
			gossip.PrintMembershipListOnce(&membershipMap)
		case "list_self":
			fmt.Println("Self Id is: " + strconv.Itoa(int(myID)))
		case "leave":
			fmt.Println("Process stopped, left the group at " + time.Now().Format("15:04:05.999999"))
			return
		case "sus":
			sus = true
			fmt.Println("Suspicion enabled")
		case "no_sus":
			sus = false
			fmt.Println("Suspicion disabled")
		case "drop":
			dropRate, _ = strconv.Atoi(fields[1])
			fmt.Printf("Drop rate set to %d\n", dropRate)
		case "no_drop":
			dropRate = 0
			fmt.Println("Drop rate set to 0%")
		case "put":
			if len(fields) != 3 {
				fmt.Println("Invalid command")
				break
			}
			localFileName := fields[1]
			sdfsFileName := fields[2]
			startTime := time.Now()
			res := sdfs.Put(localFileName, sdfsFileName, &fileTable, &localFileTable, myID, &membershipMap)
			if res == false {
				fmt.Println("Put failed")
			}
			duration := time.Since(startTime)
			fmt.Println(" --- Put time :", duration.Milliseconds(), " ms ---")
		case "get":
			if len(fields) != 3 {
				fmt.Println("Invalid command")
				break
			}
			localFileName := fields[2]
			sdfsFileName := fields[1]
			startTime := time.Now()
			res := sdfs.Get(localFileName, sdfsFileName, &fileTable, myID, &membershipMap)
			if res == false {
				fmt.Println("Get failed")
			}
			duration := time.Since(startTime)
			fmt.Println(" --- Get time :", duration.Milliseconds(), " ms ---")
		case "delete":
			sdfsFileName := fields[1]
			res := sdfs.DeleteFile(&localFileTable, sdfsFileName, &fileTable, myID, &membershipMap)
			if res == false {
				fmt.Println("Delete failed")
			}
		case "ls":
			if len(fields) != 2 {
				fmt.Println("Invalid command")
				break
			}
			sdfsFileName := fields[1]
			sdfs.Ls(sdfsFileName, &fileTable, &membershipMap)
		case "store":
			sdfs.Store(myID, &localFileTable)
		case "table":
			sdfs.PrintFileTable(&fileTable)
		case "multiread":
			if len(fields) < 3 {
				fmt.Println("Invalid command")
				break
			}
			sdfsFileName := fields[1]
			sdfs.Multiread(fields[2:], sdfsFileName, &fileTable, &membershipMap)
		case "maple":
			if len(fields) != 6 {
				fmt.Println("Invalid command")
				break
			}
			t := time.Now()
			maple_exe := fields[1]
			num_maples := fields[2]
			sdfs_intermediate_filename_prefix := fields[3]
			sdfs_src_directory := fields[4]
			interconn := fields[5]
			mapreduce.InitiateMap(maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory, interconn, &fileTable, myID, &membershipMap)
			jobID := <-jobFinishchan
			fmt.Printf("Map job %d finished, takes %v\n", jobID, time.Since(t))
		case "juice":
			if len(fields) != 6 {
				fmt.Println("Invalid command")
				break
			}
			t := time.Now()
			juice_exe := fields[1]
			num_juices := fields[2]
			sdfs_intermediate_filename_prefix := fields[3]
			sdfs_dest_filename := fields[4]
			delete_input := fields[5]
			mapreduce.InitiateReduce(juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input, &fileTable, myID, &membershipMap)
			jobID := <-jobFinishchan
			fmt.Printf("Reduce job %d finished, takes %v\n", jobID, time.Since(t))
		case "SELECT":
			if len(fields) == 6 {
				if fields[1] != "ALL" || fields[2] != "FROM" || fields[4] != "WHERE" {
					fmt.Println("Invalid command")
					break
				}
				dataset := fields[3]
				query := strings.Trim(fields[5], "\"")
				mapreduce.InitiateQuery(dataset, query, &fileTable, myID, &membershipMap)
			} else if len(fields) == 9 {
				if fields[1] != "ALL" || fields[2] != "FROM" || fields[5] != "WHERE" || fields[7] != "=" {
					fmt.Println("Invalid command")
					break
				}
				dataset1 := fields[3]
				dataset2 := fields[4]
				col1 := fields[6]
				col2 := fields[8]
				mapreduce.InitiateJoin(dataset1, dataset2, col1, col2, &fileTable, myID, &membershipMap)
			} else {
				fmt.Println("Invalid command")
				break
			}
		case "testall":
			if len(fields) != 3 {
				fmt.Println("Invalid command")
				break
			}
			t := time.Now()
			maple1 := "maple1.exe"
			maple2 := "maple2.exe"
			juice1 := "juice1.exe"
			juice2 := "juice2.exe"
			num_maples := "3"
			num_juices := "1"
			input := fields[1]
			interconn := fields[2]
			output := sdfs.SDFSFileNameToLocalFileName(interconn) + ".txt"
			mapreduce.InitiateMap(maple1, num_maples, "output"+strconv.Itoa(index), input, interconn, &fileTable, myID, &membershipMap)
			jobID1 := <-jobFinishchan
			fmt.Printf("jobID %d finished\n", jobID1)
			fmt.Printf("----------------------\n")
			mapreduce.InitiateReduce(juice1, num_juices, "output"+strconv.Itoa(index), "output"+strconv.Itoa(index+1), "0", &fileTable, myID, &membershipMap)
			jobID2 := <-jobFinishchan
			fmt.Printf("jobID %d finished\n", jobID2)
			fmt.Printf("----------------------\n")
			mapreduce.InitiateMap(maple2, num_maples, "output"+strconv.Itoa(index+2), "output"+strconv.Itoa(index+1), "#", &fileTable, myID, &membershipMap)
			jobID3 := <-jobFinishchan
			fmt.Printf("jobID %d finished\n", jobID3)
			fmt.Printf("----------------------\n")
			mapreduce.InitiateReduce(juice2, num_juices, "output"+strconv.Itoa(index+2), output, "0", &fileTable, myID, &membershipMap)
			jobID4 := <-jobFinishchan
			fmt.Printf("jobID %d finished\n", jobID4)
			fmt.Printf("----------------------\n")
			fmt.Printf("job %d %d %d %d finished, total time %v\n", jobID1, jobID2, jobID3, jobID4, time.Since(t))
			index += 3
		default:
			fmt.Println("Invalid command")
		}
	}
	wg.Wait()
}
