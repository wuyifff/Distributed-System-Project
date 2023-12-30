package sdfs

import (
	"bufio"
	"fmt"
	"net"
)

func RequestHandler(myAddr string, myID int64, localFileTable *LocalFileTable) {
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting:", err)
			return
		}
		netReader := bufio.NewReader(conn)
		netWriter := bufio.NewWriter(conn)
		netReadWriter := bufio.NewReadWriter(netReader, netWriter)
		Request, err := netReadWriter.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err)
			return
		}
		if Request == "" {
			fmt.Printf("Empty request, connect with %s closed\n", conn.RemoteAddr().String())
			continue
		}
		Request = Request[:len(Request)-1]
		switch Request {
		case "GET":
			go handleGETRequest(netReadWriter, myID, conn)
		case "PUT":
			go handlePUTRequest(netReadWriter, myID, conn, localFileTable)
		case "DEL":
			go handleDELETERequest(netReadWriter, myID, localFileTable)
		}
	}
}
