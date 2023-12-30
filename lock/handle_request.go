package lock

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

func HandleLockRequest(myAddr string, lock *Lock) {
	mu := &sync.Mutex{}
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
		defer conn.Close()
		var request LockRequest
		err = json.NewDecoder(conn).Decode(&request)
		remoteAddr := conn.RemoteAddr().String()
		switch request.MsgType {
		case READ_LOCK:
			// fmt.Printf("Received read lock request from %s\n", remoteAddr)
			go HandleReadLock(conn, lock, mu, remoteAddr)
		case WRITE_LOCK:
			// fmt.Printf("Received write lock request from %s\n", remoteAddr)
			go HandleWriteLock(conn, lock, mu, remoteAddr)
		case WRITE_UNLOCK:
			// fmt.Printf("Received write unlock request from %s\n", remoteAddr)
			go HandleWriteUNLock(conn, lock, mu, remoteAddr)
		case READ_UNLOCK:
			// fmt.Printf("Received read unlock request from %s\n", remoteAddr)
			go HandleReadUNLock(conn, lock, mu, remoteAddr)
		default:
			fmt.Println("Message not recognized, something wrong happened")
		}
	}

}
