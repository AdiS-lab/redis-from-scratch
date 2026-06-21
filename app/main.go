package main

import (
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func dataParser(input string){


}

//_____________ loop through client message ______________________________
func handleConnection(conn net.Conn){
buf := make([]byte, 1024)  // create buffer, read stream and assign to buffer, and then do logic based on that
	for{
		n,err := conn.Read(buf) //  number of bytes
		message := string(buf[:n])
		if err != nil{
			break}
		// }else if buf == "PING"{
		// 	conn.Write([]byte("+PONG\r\n"))
		// }else if buf == "ECHO"{
		// 	conn.Write([]byte("+parsedMessage\r\n"))
		// }
		fmt.Println(message)
		fmt.Println(n)
	}
}

func main() {
	//__________________________ intialize TCP connection _____________________________
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for{
		fmt.Println("made it to the start")
		conn, err := listener.Accept()
		if err!= nil{ 
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn) 
	}
}