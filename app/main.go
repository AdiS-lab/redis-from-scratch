package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {

	//__________________________ intialize TCP connection _____________________________
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for{
		conn, err := listener.Accept()
		fmt.Println(conn)
		if err!= nil{ 
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		//_____________ loop through client message ______________________________
		buf := make([]byte, 1024)  // create buffer, read stream and assign to buffer, and then do logic based on that
		for conn != nil{
			_,err := conn.Read(buf)
			if err != nil{
				break
			}
		conn.Write([]byte("+PONG\r\n"))
		}
	}
}