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

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes

//_____________ loop through client message ______________________________
func handleConnection(conn net.Conn){ //  conn is a byte slice
	for{
		var statement []string
		reader := bufio.NewReader(conn)
		t,_ := reader.ReadByte()
		n,_ := reader.ReadByte()
		initNum := int(n-'0')

		switch string(t){
		case "*": 
			reader.ReadString('$')// by pass the /r/n and $
			initial,_ := reader.ReadByte()
			initVal := int(initial - '0')
			reader.ReadString('\n')

			statement = handleRealConnection(reader, conn, initNum-1, initVal) // normalize number

		case "$": statement = handleRealConnection(reader, conn, 1, initNum)
		default:
			fmt.Println("Invalid type on first char")
			os.Exit(0)
		}

		switch statement[0]{
		case "PING": conn.Write([]byte("+PONG\r\n")) //  have to write back as byte slice
		case "ECHO":
			messageStr := string(statement[1])
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", messageStr)))
		default: 
			conn.Write([]byte("+messageNotFound\r\n"))
		}
	}
}

func handleRealConnection(reader *bufio.Reader, conn net.Conn, count int, initial int) []string {
	var statement []string  

	name := make([]byte, initial) // create a buffer to hold the new data 
	reader.Read(name)
	statement = append(statement, string(name))
	fmt.Println(statement)

	for count > 0{
		reader.ReadString('\n')

		b,_ := reader.ReadByte() 
		fmt.Println(string(b))

		if b != '$'{
			fmt.Println("Invalid type inside")
			os.Exit(0)
		}

		n,_ := reader.ReadByte()
		name := make([]byte, int(n - '0')) // create a buffer to hold the new data 
		reader.Read(name)

		statement = append(statement, string(name))

		reader.ReadByte() // bypass the last /r/n
		count--
	}
	return statement
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