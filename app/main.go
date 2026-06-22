package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes

var storage = make(map[string]string)
var lists = make(map[string][]string)
var queue []string

// _____________ loop through client message ______________________________
func handleConnection(conn net.Conn) { //  conn is a byte slice
	reader := bufio.NewReader(conn) //TCP is a stream, so as soon as data ends new comes, and the reader keeps going forward
	isQueue :=  false
	for {
		var statement []string
		t, _ := reader.ReadByte()
		n, _ := reader.ReadString('\r')
		initNum, _ := strconv.Atoi(strings.TrimSpace(n))
		fmt.Println(initNum)



		switch string(t) {
		case "*":
			reader.ReadString('$') // by pass the /r/n and $
			initial, _ := reader.ReadString('\n')
			initVal, _ := strconv.Atoi(strings.TrimSpace(initial))

			statement = handleRealConnection(reader, conn, initNum-1, initVal) // normalize number

		case "$":
			reader.ReadString('\n')
			statement = handleRealConnection(reader, conn, 1, initNum)
		default:
			fmt.Println("Invalid type on first char")
			os.Exit(0)
		}

		//______________________________ reading command __________________________________________
		switch strings.ToUpper(statement[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n")) //  have to write back as byte slice
		case "ECHO":
			messageStr := string(statement[1])
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(messageStr), messageStr)))
		case "SET":
			if len(statement) > 3 && strings.ToUpper(statement[3]) == "PX" { //  checking if they added expiry date.
				storage[statement[1]] = statement[2]
				ms, _ := strconv.Atoi(statement[4])
				fmt.Println(storage)
				go wait(statement[1], ms)
				conn.Write([]byte("+OK\r\n"))

			} else {
				storage[statement[1]] = statement[2] // use map to set pair
				conn.Write([]byte("+OK\r\n"))
			}
			fmt.Println(statement)
			fmt.Println(storage)
		case "GET":
			fmt.Println("made it here")
			storageKey := statement[1]
			value, exists := storage[storageKey]
			if exists {
				fmt.Println("made it here")
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), storage[storageKey])))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		case "RPUSH": // append new data to a list (a list is just a slice)
			listName := statement[1]
			for i := 2; i < len(statement); i++ {
				lists[listName] = append(lists[listName], statement[i])
				//create a list if don't exist and append and return the length of list in RESP format
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(lists[listName]))))
		case "LPUSH": // prepend list
			listName := statement[1]
			_, exists := lists[listName]
			var tempArr []string

			for i := len(statement) - 1; i >= 2; i-- {
				tempArr = append(tempArr, statement[i])
				//create a list if don't exist and append and return the length of list in RESP format
			}
			if exists {
				for j := 0; j < len(lists[listName]); j++ {
					tempArr = append(tempArr, lists[listName][j])
				}
			}
			lists[listName] = tempArr
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(lists[listName]))))
		case "LLEN": // get length of list
			listName := statement[1]
			_, exists := lists[listName]
			if exists {
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(lists[listName]))))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "LPOP": // to remove the first values when given something like LPOP name 2
			listName := statement[1]
			_, exists := lists[listName]
			if !exists {
				conn.Write([]byte("$-1\r\n"))
			} else if len(statement) > 2 {
				sliceNum, _ := strconv.Atoi(statement[2])
				LPOP(listName, sliceNum, conn)
			} else {
				LPOP(listName, 0, conn)
			}
		case "LRANGE": //  to find the range when given smth like LRANGE 0 5
			listName := statement[1]
			_, exists := lists[listName]
			start, _ := strconv.Atoi(statement[2])
			stop, _ := strconv.Atoi(statement[3])
			length := len(lists[listName])
			if stop > length-1 {
				stop = length - 1
			}
			if start < 0 {
				start = length + start
				if start < 0 {
					start = 0
				}
			}
			if stop < 0 {
				stop = length + stop
				if stop < 0 {
					stop = 0
				}
			}
			if !exists || start >= length || start > stop {
				conn.Write([]byte("*0\r\n"))
				continue
			}

			message := createArr(lists[listName], start, stop+1)
			conn.Write([]byte(message))
		case "BLPOP":
			listName := statement[1]
			timeout, _ := strconv.ParseFloat(statement[2], 64)
			if len(lists[listName]) > 0 {
				LPOP(listName, 0, conn)
			} else {
				go waitChange(listName, timeout, conn)
			}
		case "INCR": // increment any numerical value inside storage by 1
			storageKey := statement[1]
			_, exists := storage[storageKey]
			_, err := strconv.Atoi(storage[storageKey])
			if exists == false {
				storage[storageKey] = "1"
				conn.Write([]byte(":1\r\n"))
			} else if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))

			} else {
				// }else if(reflect.TypeOf(lists[listName]) != "int"){
				// 	conn.Write([]byte("+-1\r\n"))"
				fmt.Println("making it here and messing after")
				tempVal, _ := strconv.Atoi(storage[storageKey])
				storage[storageKey] = strconv.Itoa(tempVal + 1)
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", tempVal+1)))
			}
		case "MULTI":
			conn.Write([]byte("+OK\r\n"))
			isQueue = true
		case "EXEC":
			if isQueue == false{
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			}else if(len(queue) == 0){
				conn.Write([]byte("*0\r\n"))
			}else{
				message := createArr(queue, 0, len(queue))
				conn.Write([]byte(message))
			}
		default:
			conn.Write([]byte("+messageNotFound\r\n"))
		}
	}
}



func LPOP(listName string, sliceNum int, conn net.Conn) {
	fmt.Println("made it inside LPOP function")
	lengthList := len(lists[listName])

	if sliceNum > 0 {
		if sliceNum >= lengthList { //  check if LPOP name 2, 2 > list length
			message := createArr(lists[listName], 0, lengthList)
			conn.Write([]byte(message))
			lists[listName] = []string{}
		} else { // otherwise just POP out the first n values, and return them
			message := createArr(lists[listName], 0, sliceNum)
			conn.Write([]byte(message))
			lists[listName] = lists[listName][sliceNum:]
		}
	} else {
		tempVal := lists[listName][0]
		lists[listName] = lists[listName][1:]
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(tempVal), tempVal)))
	}
}

// __________________ poll and wait to see if length updates ________________________
func waitChange(listName string, timeout float64, conn net.Conn) {
	fmt.Println(timeout)
	fmt.Println("made it inside WaitChange")
	ticker := time.NewTicker(10 * time.Millisecond)
	deadline := time.Now().Add(time.Duration(timeout*1000) * time.Millisecond)
	fmt.Println(deadline)

	for range ticker.C { // ticker.C is a channel that sends something to go every so seconds. we want to check it's range (?)
		if len(lists[listName]) > 0 {
			val := lists[listName][0]
			lists[listName] = []string{}
			conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(listName), listName, len(val), val)))
			ticker.Stop()
			return
		}
		if timeout > 0 && time.Now().After(deadline) {
			fmt.Println(time.Now())
			conn.Write([]byte("*-1\r\n")) // send a null array
			ticker.Stop()
			return
		}
	}
}

func createArr(array []string, first int, last int) string { // used as a template to create arrays to send back
	fmt.Println("made it inside createArr function")
	index := first
	interval := last - index
	message := fmt.Sprintf("*%d\r\n", interval)

	for index < last {
		message += fmt.Sprintf("$%d\r\n%s\r\n", len(array[index]), array[index])
		index++
	}
	return message
}

func wait(key string, ms int) {
	fmt.Println("made it inside wait function")
	ticker := time.NewTicker(time.Duration(ms) * time.Millisecond)
	for range ticker.C {
		delete(storage, key)
		ticker.Stop() // set ticker that when first time runs out, just delete, and then go on.
	}
}
func handleRealConnection(reader *bufio.Reader, conn net.Conn, count int, initial int) []string {
	fmt.Println("made it inside handleRealConn function")
	var statement []string

	name := make([]byte, initial) // create a buffer to hold the new data
	reader.Read(name)
	statement = append(statement, string(name))
	reader.ReadString('\n')
	fmt.Println(count)

	for count > 0 {
		b, _ := reader.ReadByte()

		if b != '$' {
			fmt.Println("Invalid type inside")
			os.Exit(0)
		}

		n, _ := reader.ReadString('\n')
		size, _ := strconv.Atoi(strings.TrimSpace(n))

		otherName := make([]byte, size) // create a buffer to hold the new data
		reader.Read(otherName)

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

	for {
		fmt.Println("made it to the start")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
