package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"slices"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes

var storage = make(map[string]string)
var lists = make(map[string][]string)
var watchedKeys = make(map[string]string)
var data = make(map[string]string)
var slaveConnections []net.Conn


var watchCheck bool
var firstPONG bool
var firstOK bool
var masterUpdate bool 

// _____________ loop through client message ______________________________
func handleConnection(conn net.Conn, fullPort string) { //  conn is a byte slice
	reader := bufio.NewReader(conn) //TCP is a stream, so as soon as data ends new comes, and the reader keeps going forward
	var queue [][]string
	isQueue := false
	watchCheck = false
	firstPONG = false
	firstOK = false
	masterUpdate = false
	writeStatements := []string{"SET", "RPUSH", "LPUSH", "INCR", "LPOP", "BLPOP"} // defining arr of write cmds. 

	for {

		input := ""
		statement := parser(reader)
		if statement == nil {
			break
		}
		if len(statement) != 0 {
			input = statement[0]
		}

		// if setting up a flag that means we will route, but handling stoppage is weird. When does client stop 
		// sending and how to manage this. 

		// need a way to send the correct port. We have data, maybe can include something there. 

		if masterUpdate == true{
			fmt.Print("made it to the master update all good")
			if slices.Contains(writeStatements, strings.ToUpper(input)){
				for i:=0; i<len(slaveConnections); i++ {
					message := createArr(statement, 0, len(statement))
					slaveConnections[i].Write([]byte(message))
				}
			}
		} else if input == "MULTI" && isQueue == false {
			// but length is bad, then or isQueue = false
			isQueue = true
			conn.Write([]byte("+OK\r\n"))
		} else if input == "WATCH" {
			if isQueue == true {
				conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
			} else {
				for i := 1; i < len(statement); i++ {
					watchedKeys[statement[i]] = storage[statement[i]]
				}
				conn.Write([]byte("+OK\r\n"))
			}

		} else if input == "UNWATCH" {
			watchedKeys = make(map[string]string)
			watchCheck = false
			conn.Write([]byte("+OK\r\n"))

		} else if input == "DISCARD" {
			if isQueue == true {
				isQueue = false
				queue = [][]string{}
				watchedKeys = make(map[string]string)
				watchCheck = false
				conn.Write([]byte("+OK\r\n"))
			} else {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			}
		} else if input == "EXEC" {
			if isQueue == false {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
			} else if len(queue) == 0 {
				isQueue = false
				conn.Write([]byte("*0\r\n"))
			} else {
				isQueue = false
				if watchCheck {
					conn.Write([]byte("*-1\r\n"))
					queue = [][]string{}
					watchCheck = false

				} else {
					// find every key inside watched, and then check if that exists on storage
					writeArr := []string{}
					message := ""
					fmt.Println(queue)
					for i := 0; i < len(queue); i++ {
						writeVal := execute(queue[i], conn, fullPort)
						writeArr = append(writeArr, writeVal) // loop through queue, and then one by one append our message another string slice
					}
					count := len(writeArr)
					message += fmt.Sprintf("*%d\r\n", count)
					for j := 0; j < count; j++ {
						message += writeArr[j]
					}
					fmt.Println(message)
					conn.Write([]byte(message))
					queue = [][]string{}
				}
			}

		} else if strings.Split(input, " ")[0] == "FULLRESYNC" {
			fmt.Println("made it here")
			inputArr := strings.Split(input, " ")
			data["master_replid"] = inputArr[1]
			data["master_repl_offset"] = inputArr[2]

		} else if input == "PSYNC" {
			// base64 to binary
			// update the data to include the port PSYNC sends. 
			fmt.Println("made it to PSYNC alright")
			masterUpdate = true
			slaveConnections = append(slaveConnections, conn) // sets the state right so everything goes to the slave

			data, _ := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
			fmt.Println(len(data))
			conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data))) // SET, RPUSH, LPUSH, INCR, LPOP, BLPOP
		} else if len(strings.Split(input, " ")) > 2 {
			fmt.Print("got it!")
		} else if isQueue == true && len(statement) > 0 {

			queue = append(queue, statement)
			conn.Write([]byte("+QUEUED\r\n"))

		} else if input == "-p" {
			fmt.Println("making it to info")
			message := ""
			body := ""
			inputStr := fmt.Sprintf("role:%s", data["role"])
			inputStr1 := fmt.Sprintf("master_replid:%s", data["master_replid"])
			inputStr2 := fmt.Sprintf("master_repl_offset:%s", data["master_repl_offset"])

			body += inputStr
			body += inputStr1
			body += inputStr2
			message = fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
			fmt.Println(message)
			conn.Write([]byte(message))
		} else {
			if input == "" { // means nothing was sent in command, or smth happened along the way
				break
			} else {
				writeVal := execute(statement, conn, fullPort)
				if writeVal != "" {
					conn.Write([]byte(writeVal))
				}
			}
		}
	

	}
}

//______________________________ reading command __________________________________________
func execute(statement []string, conn net.Conn, fullPort string) string {
	switch strings.ToUpper(statement[0]) {
	case "PING":
		return ("+PONG\r\n") //  have to write back as byte slice
	case "ECHO":
		messageStr := string(statement[1])
		return (fmt.Sprintf("$%d\r\n%s\r\n", len(messageStr), messageStr))
	case "SET":
		if len(statement) > 3 && strings.ToUpper(statement[3]) == "PX" { //  checking if they added expiry date.
			storage[statement[1]] = statement[2]
			fmt.Println(statement[1])
			checkWatch(statement)
			ms, _ := strconv.Atoi(statement[4])
			fmt.Println(storage)
			go wait(statement[1], ms)
			return ("+OK\r\n")

		} else {
			checkWatch(statement)
			storage[statement[1]] = statement[2] // use map to set pair
			return ("+OK\r\n")
		}
	case "GET":
		fmt.Println("made it here")
		storageKey := statement[1]
		value, exists := storage[storageKey]
		if exists {
			fmt.Println("made it here")
			return (fmt.Sprintf("$%d\r\n%s\r\n", len(value), storage[storageKey]))
		} else {
			return ("$-1\r\n")
		}
	case "RPUSH": // append new data to a list (a list is just a slice)
		listName := statement[1]
		for i := 2; i < len(statement); i++ {
			lists[listName] = append(lists[listName], statement[i])
			//create a list if don't exist and append and return the length of list in RESP format
		}
		return (fmt.Sprintf(":%d\r\n", len(lists[listName])))
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
		return (fmt.Sprintf(":%d\r\n", len(lists[listName])))
	case "LLEN": // get length of list
		listName := statement[1]
		_, exists := lists[listName]
		if exists {
			return fmt.Sprintf(":%d\r\n", len(lists[listName]))
		} else {
			return (":0\r\n")
		}
	case "LPOP": // to remove the first values when given something like LPOP name 2
		listName := statement[1]
		_, exists := lists[listName]
		if !exists {
			return ("$-1\r\n")
		} else if len(statement) > 2 {
			sliceNum, _ := strconv.Atoi(statement[2])
			message := LPOP(listName, sliceNum, conn)
			return message
		} else {
			message := LPOP(listName, 0, conn)
			return message
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
			return ("*0\r\n")

		}

		message := createArr(lists[listName], start, stop+1)
		return (message)
	case "BLPOP":
		listName := statement[1]
		timeout, _ := strconv.ParseFloat(statement[2], 64)
		if len(lists[listName]) > 0 {
			message := LPOP(listName, 0, conn)
			return message
		} else {
			ch1 := make(chan string)
			go waitChange(listName, timeout, conn, ch1)
			val := <-ch1
			return val
		}
	case "INCR": // increment any numerical value inside storage by 1
		storageKey := statement[1]
		_, exists := storage[storageKey]
		_, err := strconv.Atoi(storage[storageKey])
		if exists == false {
			storage[storageKey] = "1"
			return (":1\r\n")
		} else if err != nil {
			return ("-ERR value is not an integer or out of range\r\n")

		} else {
			checkWatch(statement)
			fmt.Println("making it here and messing after")
			tempVal, _ := strconv.Atoi(storage[storageKey])
			storage[storageKey] = strconv.Itoa(tempVal + 1)
			return (fmt.Sprintf(":%d\r\n", tempVal+1))
		}
	case "INFO":
		fmt.Println("making it to info")
		message := ""
		body := ""
		inputStr := fmt.Sprintf("role:%s", data["role"])
		inputStr1 := fmt.Sprintf("master_replid:%s", data["master_replid"])
		inputStr2 := fmt.Sprintf("master_repl_offset:%s", data["master_repl_offset"])

		body += inputStr
		body += inputStr1
		body += inputStr2
		message = fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
		fmt.Println(message)
		return message
	case "PONG":
		if firstPONG == false {
			firstPONG = true
			return fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", fullPort)
		}
		return ""
	case "OK":
		if firstOK == false {
			firstOK = true
			return ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
		}
		return "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	case "REPLCONF": 
		// if have multiple replicas there has to be a way to identify which, wait no, sending to all replicas that's the
		// point. So maybe data is supposed to be an arr, with slave connections all there, and then just write to all of them
		return "+OK\r\n"
	default:
		return ("+messageNotFound\r\n")
	}
}

//  set and increment

func LPOP(listName string, sliceNum int, conn net.Conn) string {
	fmt.Println("made it inside LPOP function")
	lengthList := len(lists[listName])

	if sliceNum > 0 {
		if sliceNum >= lengthList { //  check if LPOP name 2, 2 > list length
			message := createArr(lists[listName], 0, lengthList)
			lists[listName] = []string{}
			return message

		} else { // otherwise just POP out the first n values, and return them
			message := createArr(lists[listName], 0, sliceNum)
			lists[listName] = lists[listName][sliceNum:]
			return message
		}
	} else {
		tempVal := lists[listName][0]
		lists[listName] = lists[listName][1:]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(tempVal), tempVal)
	}
}
func checkWatch(statement []string) {
	_, exists := watchedKeys[statement[1]]
	if exists {
		watchCheck = true
		if watchedKeys[statement[1]] == "" {
			watchedKeys[statement[1]] = statement[2]
		}
	}
}
// __________________ poll and wait to see if length updates ________________________
func waitChange(listName string, timeout float64, conn net.Conn, ch1 chan string) {
	fmt.Println(timeout)
	fmt.Println("made it inside WaitChange")
	ticker := time.NewTicker(10 * time.Millisecond)
	deadline := time.Now().Add(time.Duration(timeout*1000) * time.Millisecond)
	fmt.Println(deadline)

	for range ticker.C { // what does ticker c represent?
		if len(lists[listName]) > 0 {
			val := lists[listName][0]
			lists[listName] = []string{}
			tempVal := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(listName), listName, len(val), val)
			ch1 <- tempVal
			ticker.Stop()
		}
		if timeout > 0 && time.Now().After(deadline) {
			fmt.Println(time.Now())
			tempVal := "*-1\r\n" // send a null array
			fmt.Println(tempVal)
			ch1 <- tempVal
			ticker.Stop()
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
func parser(reader *bufio.Reader) []string {
	fmt.Println("made it insider paser !!!")

	// 3 different versions $n \r\n         *n \r\n $b \r\n
	t, err := reader.ReadByte() // read first byte
	count := 0
	var initVal int
	var statement []string
	fmt.Println(string(t))
	if err != nil {
		return nil
	}

	switch string(t) {
	case "*":
		n, _ := reader.ReadString('\r')
		count, _ = strconv.Atoi(strings.TrimSpace(n)) // got the count \n $b \r\n
		count = count - 1                             // subtract 1 because we already calculating first value
		reader.ReadString('$')                        // by pass the /r/n and $

		initial, _ := reader.ReadString('\n')
		initVal, _ = strconv.Atoi(strings.TrimSpace(initial)) // this for my first word.
	case "$":
		initial, err := reader.ReadString('\n')
		tempVal, err := strconv.Atoi(strings.TrimSpace(initial)) // got the count \n $b \r\n
		// fmt.Println("RDB length: ", tempVal, "err:", err)
		if err != nil {
			return statement
		}

		buf := make([]byte, tempVal) // we set a buffer
		io.ReadFull(reader, buf)     // consume and discard
		reader.ReadByte()
		return statement
		// reader.ReadString('\n')
	case "+":
		word, _ := reader.ReadString('\n')
		statement = append(statement, strings.TrimSpace(word))
		fmt.Println(statement)
		return statement
	default:
		fmt.Println("Invalid type on first char ", t)
		fmt.Println("Invalid type on first char")
		os.Exit(0)
	}
	// redis-cli -p 6380 INFO replication

	//____________________ let's apply the same logic, so the above is choosing arr or str __________________________

	name := make([]byte, initVal) // create a buffer to hold the new data
	reader.Read(name)
	statement = append(statement, string(name))
	reader.ReadString('\n')

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
	fmt.Println("THIS IS STATEMENT")
	fmt.Println(statement)
	return statement
}

// want to check if key exists inside the watching storage

func main() {
	//__________________________ intialize TCP connection _____________________________
	fullPort := "6379"
	data["master_repl_offset"] = "0"
	data["role"] = "master"
	data["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

	if len(os.Args) > 2 {
		if os.Args[1] == "--port" {
			fullPort = os.Args[2]
		}
	}
	if len(os.Args) > 3 {
		if os.Args[3] == "--replicaof" {
			data["role"] = "slave"
		}
		fmt.Println(os.Args[4]) // comes in as localhost 6479 without the :
		//split or loop through it. or manually do it.

	}
	if len(os.Args) > 4 {
		host := os.Args[4][0:9]
		port := os.Args[4][10:]

		masterConn, err := net.Dial("tcp", host+":"+port)
		if err != nil {
			fmt.Println(err.Error())
		}

		masterConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		// reader := bufio.NewReader(masterConn)
		fmt.Println("Made it inside this for loop")
		go handleConnection(masterConn, fullPort)
		// so we establish our master connection assuming this is a replica here. 
		// master gets port based on message sente
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+fullPort)
	fmt.Println(listener)
	if err != nil {
		fmt.Println("Failed to bind to port")
		os.Exit(1)
	}

	for {
		fmt.Println("made it to the start")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, fullPort) //  connection made, this is used to parse everything now.
	}
}
