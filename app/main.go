package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes


var storage = make(map[string]string)


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

		case "$": 
			reader.ReadString('\n')
			statement = handleRealConnection(reader, conn, 1, initNum)
		default:
			fmt.Println("Invalid type on first char")
			os.Exit(0)
		}

		switch strings.ToUpper(statement[0]){
		case "PING": conn.Write([]byte("+PONG\r\n")) //  have to write back as byte slice
		case "ECHO":
			messageStr := string(statement[1])
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(messageStr), messageStr)))
		case "SET":
			if strings.ToUpper(statement[3]) == "PX"{ //  checking if they added expiry date. 
				storage[statement[1]] = statement[2]
				ms := int(statement[5])
				ticker := time.NewTicker(ms * (time.Second/1000))
				for range ticker.C{
					delete(storage, statement[1])
					ticker.Stop() // set ticker that when first time runs out, just delete, and then go on. 
				}
 			}else{
				storage[statement[1]] = statement[2] // use map to set pair
			}
			fmt.Println(statement)
			fmt.Println(storage)
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			value, exists := storage[statement[1]]
			if exists{
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}else{
				conn.Write([]byte("$-1\r\n"))
			}
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
	reader.ReadString('\n')

	fmt.Println(string(name))

	for count > 0{
		b,_ := reader.ReadByte() 

		if b != '$'{
			fmt.Println("Invalid type inside")
			os.Exit(0)
		}

		n,_ := reader.ReadByte()
		reader.ReadString('\n')

		name := make([]byte, int(n - '0')) // create a buffer to hold the new data 
		reader.Read(name)

		statement = append(statement, string(name))

		reader.ReadString('\n') // bypass the last /r/n
		count--
	}
	fmt.Println(statement)
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