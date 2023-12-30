package mapreduce

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

func sendRequestJSON(conn net.Conn, msg interface{}) error {
	marshal, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("Error marshaling message: %v\n", err)
		return err
	}
	dataLength := len(marshal)
	lengthbuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthbuf, uint32(dataLength))
	if conn == nil {
		return errors.New("Connection is closed")
	}
	_, err = conn.Write(lengthbuf)
	if err != nil {
		fmt.Println("Error writing length prefix:", err)
		return err
	}
	_, err = conn.Write(marshal)
	if err != nil {
		fmt.Println("Error writing Json data:", err)
		return err
	}
	// fmt.Printf("Sending message: %v\n", msg)
	return nil
}

func receiveMapReduceRequestJSON(conn net.Conn) (MapReduceMsg, error) {
	var request MapReduceMsg
	lengthPrefix := make([]byte, 4)
	_, err := conn.Read(lengthPrefix)
	if err != nil {
		fmt.Println("Error reading length prefix:", err)
		return MapReduceMsg{}, err
	}
	dataLength := int(binary.BigEndian.Uint32(lengthPrefix))
	jsonBuffer := make([]byte, dataLength)
	_, err = conn.Read(jsonBuffer)
	if err != nil {
		fmt.Println("Error reading Json data:", err)
		return MapReduceMsg{}, err
	}
	err = json.Unmarshal(jsonBuffer, &request)
	if err != nil {
		fmt.Println("Error unmarshaling for MR request:", err)
		switch err := err.(type) {
		case *json.SyntaxError:
			fmt.Printf("JSON syntax error at offset %d: %s\n", err.Offset, err)
		case *json.UnmarshalTypeError:
			fmt.Printf("type error at offset %d: expected %v, actual %v\n", err.Offset, err.Type, err.Value)
		}
		return MapReduceMsg{}, err
	}
	return request, nil
}
