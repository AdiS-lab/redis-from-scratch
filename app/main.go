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
	"path/filepath"
	"encoding/binary"
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
var slaveConnections = make(map[net.Conn]map[string]string) // sync.Mutex protects concurrent access (whateva that means)
var configs = make(map[string]string)

var watchCheck bool
var firstPONG bool
var firstOK bool
var masterUpdate bool 
var expectingRDB = false

// _____________ loop through client message ______________________________
func handleConnection(conn net.Conn, fullPort string) { //  conn is a byte slice
	reader := bufio.NewReader(conn) //TCP is a stream, so as soon as data ends new comes, and the reader keeps going forward
	var queue [][]string
	isQueue := false
	watchCheck = false
	firstPONG = false
	firstOK = false
	expectingRDB = false
	writeStatements := []string{"SET", "RPUSH", "LPUSH", "INCR", "LPOP", "BLPOP"} // defining arr of write cmds. 
	
	
	directory := configs["dir"] 
	filePath := configs["dbfilename"]
	fullPath := filepath.Join(directory, filePath)
	info,_ := os.ReadFile(fullPath) //create byte arr
	if info == nil{
	}
	allKeys, allVals, allExp  := readRDB(info)
	fmt.Println("this is allkeys again ", allKeys) 
	fmt.Println("this is allVals ", allVals) 
	fmt.Println("this is allExp ", allExp)
	for i:=0; i<len(allKeys); i++{
		storage[allKeys[i]] = allVals[i]
		if (allExp[i] != ""){
			fmt.Println("have made it inside the expiry handler ")
			ms,_ := strconv.Atoi(allExp[i])
			go waitKey(allKeys[i], ms)
		}
	}



	for {


		input := ""
		statement, recreatedCmd := parser(reader) // if nil break
		if statement == nil {
			fmt.Println("something happened and connection disconnected")
			break
		}
		if len(statement) != 0 {
			input = statement[0]
		}
		// manage masterUpdate by checking when doesn't equal one of those. 


		fmt.Println("before going into check is ", masterUpdate)
		if masterUpdate == true && data["role"] == "master"{//after three way connection
			fmt.Println("propogating down to slave here's statement ", statement)
			if slices.Contains(writeStatements, strings.ToUpper(input)){
				curr_offset,_ := strconv.Atoi(data["master_repl_offset"])
				new_offset := curr_offset + len(recreatedCmd)
				data["master_repl_offset"] = strconv.Itoa(new_offset)
				for conn, _ := range slaveConnections {
					message := createArr(statement, 0, len(statement))
					fmt.Println("for propogation message is ", message)
					conn.Write([]byte(message))

				}
			}
	
		} 

		if input == "MULTI" && isQueue == false {//set queue as long as no tin queue
			// but length is bad, then or isQueue = false
			isQueue = true
			conn.Write([]byte("+OK\r\n"))
		} else if input == "WATCH" {// set keys that can't be changed
			if isQueue == true {
				conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
			} else {
				for i := 1; i < len(statement); i++ {
					watchedKeys[statement[i]] = storage[statement[i]]
				}
				conn.Write([]byte("+OK\r\n"))
			}

		} else if input == "UNWATCH" {// reset, remove all watched keys
			watchedKeys = make(map[string]string)
			watchCheck = false
			conn.Write([]byte("+OK\r\n"))

		} else if input == "DISCARD" {// remove all queued items reset
			if isQueue == true {
				isQueue = false
				queue = [][]string{}
				watchedKeys = make(map[string]string)
				watchCheck = false
				conn.Write([]byte("+OK\r\n"))
			} else {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			}
		} else if input == "EXEC" {//if queued items then start executing them 
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

		} else if strings.Split(input, " ")[0] == "FULLRESYNC" {// second step of 3 way handshake for master
			inputArr := strings.Split(input, " ")
			data["master_replid"] = inputArr[1]
			data["master_repl_offset"] = inputArr[2]
			expectingRDB = true
		} else if input == "PSYNC" { // 3rd step for 3 way handshake
			// base64 to binary
			// update the data to include the port PSYNC sends. 
			masterUpdate = true
			slaveConnections[conn] = map[string]string{} // sets the state right so everything goes to the slave
			slaveConnections[conn]["offset"] = "0"
			
			data, _ := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
			conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")) // send FULL RESYNC
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data))) // send RDB file
		}else if isQueue == true && len(statement) > 0 {// to actually put stuff inside our queue

			queue = append(queue, statement)
			conn.Write([]byte("+QUEUED\r\n"))

		} else {
			if input == "" { // means nothing was sent in command, or smth happened along the way
				continue
			} else {
				writeVal := execute(statement, conn, fullPort)
				if(masterUpdate && data["role"] == "slave"){//in case of slave + needing to update offset
					curr_offset,_ := strconv.Atoi(data["master_repl_offset"])
					new_offset := curr_offset + len(recreatedCmd)

					fmt.Println("made it inside to the update offset ",curr_offset)
					data["master_repl_offset"] = strconv.Itoa(new_offset)
				}
				if writeVal != "" {
					fmt.Println("this is writeVal ", writeVal, data["role"])
					//after processing update offest, which mean reconvert that jawn 
					// 1. need way to process
					// 2. figure out where to update
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
		fmt.Println("made it inside PING at least")
		return writeUpdate("+PONG\r\n") //  have to write back as byte slice
	case "ECHO":
		messageStr := string(statement[1])
		return (fmt.Sprintf("$%d\r\n%s\r\n", len(messageStr), messageStr))
	case "SET":
		fmt.Println("made it to SET and command is ", statement)
		if len(statement) > 3 && strings.ToUpper(statement[3]) == "PX" { //  checking if they added expiry date.
			storage[statement[1]] = statement[2]
			fmt.Println(statement[1])
			checkWatch(statement)
			ms, _ := strconv.Atoi(statement[4])
			fmt.Println(storage)
			go wait(statement[1], ms)
			return writeUpdate("+OK\r\n")

		} else {
			checkWatch(statement)
			storage[statement[1]] = statement[2] // use map to set pair
			fmt.Println("this is storage after setting ", storage)
			return writeUpdate("+OK\r\n")
		}
	case "GET":
		fmt.Println("made it to GET cmd")
		fmt.Println(storage)
		fmt.Println(slaveConnections)
		storageKey := statement[1]
		value, exists := storage[storageKey]
		if exists {
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
		return writeUpdate(fmt.Sprintf(":%d\r\n", len(lists[listName])))
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
			return writeUpdate("$-1\r\n")
		} else if len(statement) > 2 {
			sliceNum, _ := strconv.Atoi(statement[2])
			message := LPOP(listName, sliceNum, conn)
			return writeUpdate(message)
		} else {
			message := LPOP(listName, 0, conn)
			return writeUpdate(message)
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
			return writeUpdate(message)
		} else {
			ch1 := make(chan string)
			go waitChange(listName, timeout, conn, ch1)
			val := <-ch1 // create this because we are waiting for list to be updated, so have to use channel to communicate
			return writeUpdate(val)
		}
	case "INCR": // increment any numerical value inside storage by 1
		storageKey := statement[1]
		_, exists := storage[storageKey]
		_, err := strconv.Atoi(storage[storageKey])
		if exists == false {
			storage[storageKey] = "1"
			return (":1\r\n")
		} else if err != nil {
			return writeUpdate("-ERR value is not an integer or out of range\r\n")

		} else {
			checkWatch(statement)
			fmt.Println("making it here and messing after")
			tempVal, _ := strconv.Atoi(storage[storageKey])
			storage[storageKey] = strconv.Itoa(tempVal + 1)
			return writeUpdate(fmt.Sprintf(":%d\r\n", tempVal+1))
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
		fmt.Println("made it inside ReplConf ", statement)
		if(len(statement)>2 && statement[1] == "GETACK"){
			fmt.Println("made it to GETACK")
			offset := data["master_repl_offset"]
			return fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offset), offset)
		}else if(len(statement)>2 && statement[1] == "ACK"){
			fmt.Println("Recieved ACK statement ", statement[2])
			slaveConnections[conn]["offset"] = statement[2]
			return ""
		}
		return "+OK\r\n"
	case "WAIT":
		if(data["role"] == "master"){
		
			// either after time is expired, or if completed before
			for connection,_ := range slaveConnections {
				connection.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")) // continiously sends this out every ticker second, and if received, will 
			}
			target,_:= strconv.Atoi(statement[1])
			sleep,_ := strconv.Atoi(statement[2])
			ch := make(chan string)
			go waitOnConnections(sleep, target, ch)
			return <- ch
		}
		return""
    case "CONFIG": 
		files := statement[2]
		directory := configs["dir"] 
		filePath := configs["dbfilename"]
		
		switch files{
		case "dir":  
			return fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n",len(directory),directory)
		case "dbfilename":
			return fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(filePath), filePath)

		case "appendonly":
			return fmt.Sprintf("*2\r\n$10\r\nappendonly\r\n$%d\r\n%s\r\n",len(configs["appendonly"]),  configs["appendonly"]) // appendonly, no
		case "appenddirname":
			return fmt.Sprintf("*2\r\n$13\r\nappenddirname\r\n$%d\r\n%s\r\n", len(configs["appenddirname"]),  configs["appenddirname"])// appenddirname, appendonlydir

		case "appendfilename":
			return fmt.Sprintf("*2\r\n$14\r\appendfilename\r\n$%d\r\n%s\r\n", len(configs["appendfilename"]), configs["appendfilename"]) // appendfilename, appendonly.aof
		case "appendfsync":
			return fmt.Sprintf("*2\r\n$11\r\appendfsync\r\n$%d\r\n%s\r\n", len(configs["appendfsync"]), configs["appendfsync"])// everysec
		default:
			return ""
		}
	
		case "KEYS":	
			directory := configs["dir"] 
			filePath := configs["dbfilename"]
			fullPath := filepath.Join(directory, filePath)
			info,_ := os.ReadFile(fullPath) //create byte arr
			if info == nil{
				return ""
			}
			allKeys, _ , _ := readRDB(info)
			decide := statement[1]
			switch decide{
			case "*": 
				fmt.Println("here are all the keys ", allKeys)
				message := createArr(allKeys, 0, len(allKeys))
				return message
			}
			return ""
	default:
		return ("+messageNotFound\r\n")
	}
}

// we have different cases, and are reading the beginning of the string
// this means that

func readRDB(info []byte)([]string, []string, []string){
	i:= 0
	count:=0

	allKeys := []string{}
	allVals := []string{}
	allExp := []string{}


	for i<len(info){
		 // different ways to parse. Have to find where key value store starts, and then 
		 // find the length of the key value store, then we have to find a 00 
		 // then we can create a slice
		if info[i] == 0xFB{
			length := int(info[i+1])
			fmt.Println("this is the length given by oxfb ", length)
			allExp = make([]string, length)
			i = i+3
			for j:=0; j<length;j++{
				fmt.Println("this is where we are ", i, len(info))
				if info[i] == 0xFC{
					fmt.Println("")

					// unix timestamp uses little endian, so backwards by size of bytes. 
					tempExp := binary.LittleEndian.Uint64(info[i+1:i+9]) 
					expiry := strconv.FormatUint(tempExp, 10)
					allExp[count] = expiry
					i = i + 9
				} // at this point at 0x00
				realLen := int(info[i+1])
				keyLen := i+2+realLen
				keys := info[i+2:keyLen]

				realLen2 := int(info[keyLen])
				vals := info[keyLen+1: keyLen+1 + realLen2]

				fmt.Println("this is key ", string(keys))
				fmt.Println("this is value ", string(vals))

				allKeys = append(allKeys, string(keys))
				allVals = append(allVals, string(vals))
				count ++ 
				i =  i + 3 + realLen + realLen2
			}
		}else{
			i++
		}
	}

	fmt.Println("this is all Keys ", allKeys)
	return allKeys, allVals, allExp
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
		watchCheck = true // somethign was modified, this it what it means
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
func waitKey(key string, ms int) {
	fmt.Println("made it inside wait function")
	expiryTime := time.UnixMilli(int64(ms))
	if(time.Now().After(expiryTime)){
		delete(storage, key)
	} // in the case that it is in unix
}

func wait(key string, ms int){
	deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(10) *( time.Millisecond))
	fmt.Println(" here are the keys to be deleted ", key)

	for range ticker.C{
		if time.Now().After(deadline){
			delete(storage, key)
			// ticker.Stop() // set ticker that when first time runs out, just delete, and then go on.
		}
	}
}
func parser(reader *bufio.Reader)( []string, string) {
	// 3 different versions $n \r\n         *n \r\n $b \r\n
	t, err := reader.ReadByte() // read first byte
	count := 0
	recreatedCmd := string(t)
	var initVal int
	statement := []string{}
	fmt.Println("starting to parse, the start char is ", string(t))

	if err != nil {
		return nil, ""
	}

	switch string(t) {
	case "*":
		n, _ := reader.ReadString('\r')
		recreatedCmd += n

		count, _ = strconv.Atoi(strings.TrimSpace(n)) // got the count \n $b \r\n
		count = count - 1                             // subtract 1 because we already calculating first value
		r,_ := reader.ReadString('$')                        // by pass the /r/n and $
		recreatedCmd += r

		initial, _ := reader.ReadString('\n')
		recreatedCmd += initial

		initVal, _ = strconv.Atoi(strings.TrimSpace(initial)) // this for my first word.
	case "$":
		initial, err := reader.ReadString('\n') // handle case that it is RDB file.
		recreatedCmd += initial

		initVal, err := strconv.Atoi(strings.TrimSpace(initial)) // got the count \n $b \r\n
		if err != nil {
			fmt.Println("error here when trying to parse RDB ", err)
			return nil,""
		}
		fmt.Println("expectingRDB inside parse is ", expectingRDB)
		if(expectingRDB){ //handle RDB file
			fmt.Println("this is initVal ", initVal)
			buf := make([]byte, initVal) // we set a buffer
			io.ReadFull(reader, buf)   // consume and discard
			expectingRDB = false
			masterUpdate = true
			return statement, recreatedCmd
		}
	case "+":
		word, _ := reader.ReadString('\n')
		recreatedCmd += word

		statement = append(statement, strings.TrimSpace(word))
		fmt.Println(statement)
		return statement, recreatedCmd
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
	recreatedCmd += string(name)

	e,_ := reader.ReadString('\n')
	recreatedCmd += e

	for count > 0 {
		b, _ := reader.ReadByte()
		recreatedCmd += string(b)

		if b != '$' {
			fmt.Println("Invalid type inside")
			os.Exit(0)
		}

		n, _ := reader.ReadString('\n')
		recreatedCmd += n
		size, _ := strconv.Atoi(strings.TrimSpace(n))

		otherName := make([]byte, size) // create a buffer to hold the new data, bc don't know where ends
		reader.Read(otherName)
		statement = append(statement, string(otherName))
		recreatedCmd += string(otherName)

		f, _ := reader.ReadString('\n') // bypass the last /r/n
		recreatedCmd += f
		count--
	}
	fmt.Println("immediately after parsing cmd is " , recreatedCmd)
	fmt.Println("immediately after parsing statement is ", statement)
	return statement, recreatedCmd
}
func writeUpdate(returnVal string) string{
	if data["role"] == "master" {
		return returnVal
	}else{
		fmt.Println("made it here as a slave to writeUpdate")
		return ""
	}
}
func waitOnConnections(sleep int, target int, ch chan string){
	deadline := time.Now().Add(time.Duration(sleep)*time.Millisecond)
	ticker := time.NewTicker(time.Duration(10) * time.Millisecond)
	count := 0
	masterOffset,_ := strconv.Atoi(data["master_repl_offset"])

	// need to send REPLGEETACK
	
	for range ticker.C{
		fmt.Println("this is masterOffset", masterOffset)
		if(time.Now().After(deadline)){ 
			fmt.Println("made it to the point where it overextended")
			ch <- fmt.Sprintf(":%d\r\n", count) // go into infinite for loop wait until after deadline
			ticker.Stop()
			break
		}else{ // keep resetting such can count from fresh. 
			count = 0
			for conn,_ := range slaveConnections{
				offsetVal,_ := strconv.Atoi(slaveConnections[conn]["offset"])
				fmt.Println("this is Slave offset ", offsetVal)
				if(offsetVal>=masterOffset){
					count++ 
				}
			}
			if(count>=target){
				ch <- fmt.Sprintf(":%d\r\n", count)
				ticker.Stop()
				break
			}
		}
	}
}

// want to check if key exists inside the watching storage

func main() {
	//__________________________ intialize TCP connection _____________________________
	fullPort := "6379"
	data["master_repl_offset"] = "0"
	data["role"] = "master"
	data["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

	curr_dir, _ := os.Getwd()
	fmt.Println("this is the current direcotry " , curr_dir)
	configs["dir"] = curr_dir
	configs["appendonly"] = "no"
	configs["appenddirname"] = "appendonlydir"
	configs["appendfilename"] = "appendonly.aof"
	configs["appendfsync"] = "everysec"

	if len(os.Args) > 2 {
		if os.Args[1] == "--port" || os.Args[1] == "-p" {
			fullPort = os.Args[2]
		}else if(os.Args[1]=="--dir"){
			configs["dir"] = os.Args[2] // dir is supposed to be the directory where it is found
			configs["dbfilename"] = os.Args[4]
		}
	}

	if len(os.Args) > 3 {
		if os.Args[3] == "--replicaof" {
			data["role"] = "slave"
			if len(os.Args) > 4 {
				host := os.Args[4][0:9]
				port := os.Args[4][10:]

				masterConn, err := net.Dial("tcp", host+":"+port)
				if err != nil {
					fmt.Println(err.Error())
				}

				masterConn.Write([]byte("*1\r\n$4\r\nPING\r\n")) //send ping as slave
				// reader := bufio.NewReader(masterConn)
				fmt.Println("Made it inside this for loop")
				go handleConnection(masterConn, fullPort)
				// so we establish our master connection assuming this is a replica here. 
				// master gets port based on message sente
			}
		}
		fmt.Println(os.Args[4]) // comes in as localhost 6479 without the :
		//split or loop through it. or manually do it.
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+fullPort)
	fmt.Println(listener)
	if err != nil {
		fmt.Println("Failed to bind to port")
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, fullPort) //  connection made, this is used to parse everything now.
	}
}
