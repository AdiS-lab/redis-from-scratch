package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"strconv"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes


var storage = make(map[string]string)
var lists = make(map[string][]string)

//_____________ loop through client message ______________________________
func handleConnection(conn net.Conn){ //  conn is a byte slice
	fmt.Println(conn)
	reader := bufio.NewReader(conn) //TCP is a stream, so as soon as data ends new comes, and the reader keeps going forward 
	for{
		var statement []string
		t,_ := reader.ReadByte()
		n,_ := reader.ReadByte()
		initNum := int(n-'0')

		switch string(t){
		case "*": 
			reader.ReadString('$')// by pass the /r/n and $
			initial,_ := reader.ReadString('\n')
			initVal,_ := strconv.Atoi(strings.TrimSpace(initial))

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
			if len(statement) > 3 && strings.ToUpper(statement[3]) == "PX"{ //  checking if they added expiry date. 
				storage[statement[1]] = statement[2]
				ms,_ := strconv.Atoi(statement[4])
				fmt.Println(storage)
				go wait(statement[1], ms)
				conn.Write([]byte("+OK\r\n"))

 			}else{
				storage[statement[1]] = statement[2] // use map to set pair
				conn.Write([]byte("+OK\r\n"))
			}
			fmt.Println(statement)
			fmt.Println(storage)
		case "GET":
			fmt.Println("made it here")
			value, exists := storage[statement[1]]
			if exists{
				fmt.Println("made it here")
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}else{
				conn.Write([]byte("$-1\r\n"))
			}
		case "RPUSH":
			listName := statement[1]
			for i:=2; i<len(statement); i++ {
				lists[listName] = append(lists[listName], statement[i])
				//create a list if don't exist and append and return the length of list in RESP format
			}
			conn.Write([]byte( fmt.Sprintf(":%d\r\n", len(lists[listName])) ))

		case "LPUSH" :{
			listName := statement[1]	
			value, _ = lists[listName]
			var tempArr []string

			for i:=len(statement)-1; i>=2; i-- {
				tempArr = append(tempArr, statement[i])
				//create a list if don't exist and append and return the length of list in RESP format
			}
			tempArr = append(tempArr, value)
			lists[listName] = tempArr
			conn.Write([]byte( fmt.Sprintf(":%d\r\n", len(lists[listName])) ))
		}

		case "LRANGE":
			listName := statement[1]
			_, exists := lists[listName]
			start,_ := strconv.Atoi(statement[2])
			stop,_ := strconv.Atoi(statement[3])
			length := len(lists[listName])
			var message string

		 	if stop>length-1{
				stop = length-1
			}
			if start < 0{
				start = length + start
				if start <0{
					start = 0 
				}
			}
			if stop < 0{
				stop = length + stop
				if stop < 0{
					stop = 0
				}
			}
			if !exists || start >= length || start>stop{
				conn.Write([]byte("*0\r\n"))
				continue
			}
		
			interval:=stop-start+1
			message = fmt.Sprintf("*%d\r\n", interval )
			for i:=start; i<=stop; i++ {
				val := lists[listName][i]
				message += fmt.Sprintf("$%d\r\n%s\r\n", len(val), val) 
			}
			conn.Write([]byte(message))	
			

		default: 
			conn.Write([]byte("+messageNotFound\r\n"))
		}
	}
}

func wait(key string, ms int){
	ticker := time.NewTicker(time.Duration(ms) * time.Millisecond)
	for range ticker.C{
		delete(storage, key)
		ticker.Stop() // set ticker that when first time runs out, just delete, and then go on.
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
		fmt.Println(statement)

		if b != '$'{
			fmt.Println("Invalid type inside")
			os.Exit(0)
		}

		n,_ := reader.ReadString('\n')
		size,_ := strconv.Atoi(strings.TrimSpace(n))

		otherName := make([]byte, size) // create a buffer to hold the new data 
		reader.Read(otherName)
		fmt.Println(string(otherName))

		statement = append(statement, string(otherName))

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
		fmt.Println(conn)
		go handleConnection(conn) 
	}
}