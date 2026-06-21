package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// func dataParser(input string){
// 	reader := bufio.readByte(conn)
// 	b,_ = reader.readByte() 
// }

//_____________ loop through client message ______________________________
// func handleConnection(conn net.Conn){
// buf := make([]byte, 1024)  // create buffer, read stream and assign to buffer, and then do logic based on that
// 	for{
// 		n,err := conn.Read(buf) //  number of bytes
// 		message := string(buf[:n])
// 		if err != nil{
// 		break
// 		}
// 		message := string(buf[:n])
// 		switch message[0]: 
// 		case "*" 
// 		case "$" 
// 		fmt.Println(message)
// 		fmt.Println(n)
// 	}
// }

func handleRealConnection(conn net.Conn){
	reader := bufio.NewReader(conn)
	b,_ := reader.ReadByte() 
	fmt.Println(string(b))
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
		go handleRealConnection(conn) 
	}
}