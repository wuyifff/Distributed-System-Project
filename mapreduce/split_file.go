package mapreduce

import (
	"bufio"
	"fmt"
	"main/gossip"
	"main/sdfs"
	"os"
	"strings"
	"time"
)

// splitFile splits the file into num_nodes parts, and save them as split_1.txt, split_2.txt, etc. into the local directory
func splitFile(nodeMap map[int64]bool, src string, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, myID int64, jobID int) error {
	// 1. leader get the file
	num_nodes := len(nodeMap)
	fileInDir := sdfs.LsFolder(src, fileTable)
	var inputFiles []*os.File
	for _, file := range fileInDir {
		dirs := strings.Split(file, "/")
		filename := dirs[len(dirs)-1]
		ret := false
		ret = sdfs.Get(filename, file, fileTable, myID, membershipMap)
		for !ret {
			time.Sleep(1 * time.Second)
			ret = sdfs.Get(filename, file, fileTable, myID, membershipMap)
		}
		ptr, err := os.Open(filename)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return err
		}
		inputFiles = append(inputFiles, ptr)
		defer ptr.Close()
	}

	// 2. leader split the file, split without truncate
	var splited_files []*os.File
	for key := range nodeMap {
		outputFile, err := os.Create(fmt.Sprintf("split_%d_%d", jobID, key))
		if err != nil {
			fmt.Println("Error creating file:", err)
			return err
		}
		splited_files = append(splited_files, outputFile)
		defer outputFile.Close()
	}
	curFile := 0
	for _, inputfile := range inputFiles {
		scanner := bufio.NewScanner(inputfile)
		for scanner.Scan() {
			line := scanner.Text()
			splited_files[curFile].WriteString(line + "\n")
			curFile = (curFile + 1) % num_nodes
		}
	}
	return nil
}
