package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Entry struct {
	Member string
	Score  float64
} // define data structure for ZADD commnand

type FullList struct {
	Entry
	Name string
}

type User struct {
	Passwords  []string
	Flags      []string
	Connection net.Conn
}

// e := Entry{member:, score: }
// push this into a list/object

// buf := make([]byte, 1024)  create buffer, read stream and assign to buffer, and then do logic based on that
// n,err := conn.Read(buf)  number of bytes

var storage = make(map[string]string)
var lists = make(map[string][]string)
var watchedKeys = make(map[string]string)
var data = make(map[string]string)
var slaveConnections = make(map[net.Conn]map[string]string) // sync.Mutex protects concurrent access (whateva that means)
var configs = make(map[string]string)
var expired = make(map[string]int)
var totalSubs = make(map[string][]net.Conn)
var sortedSets = make(map[string][]Entry) // in the key we should have a list populated by multiple entries, if we want to create a new one, we just do so.
var users = make(map[string]User)         // map each connection to a user
var streams = make(map[string]map[string]map[string]string)

//	name: {
//		id: {
//			key: value
//			key: valye
//			key: value
//		}
//		id{
//			key: value
//			key: value
//		}
//	}
var authState = true

var watchCheck bool
var firstPONG bool
var firstOK bool
var masterUpdate bool
var expectingRDB = false

// _____________ loop through client message ______________________________
func handleConnection(conn net.Conn, fullPort string) { //  conn is a byte slice
	users["default"] = User{Connection: conn, Passwords: []string{}, Flags: []string{"nopass"}}

	reader := bufio.NewReader(conn) //TCP is a stream, so as soon as data ends new comes, and the reader keeps going forward
	var queue [][]string
	var subscribeMode bool
	var channelArr []string

	prev_id := "0-0" // every stream has to have their own. if stream don't exist then reset prev_id
	//
	isQueue := false
	watchCheck = false
	firstPONG = false
	firstOK = false
	expectingRDB = false
	userAuth := false

	writeStatements := []string{"SET", "RPUSH", "LPUSH", "INCR", "LPOP", "BLPOP"} // defining arr of write cmds.
	subStatements := []string{"SUBSCRIBE", "PUBLISH", "UNSUBSCRIBE"}

	otherDir := configs["dir"]
	filePath := configs["dbfilename"]
	fullPath := filepath.Join(otherDir, filePath)
	info, _ := os.ReadFile(fullPath) //create byte arr

	allKeys, allVals, allExp := readRDB(info)
	fmt.Println("this is allkeys again ", allKeys)
	fmt.Println("this is allVals ", allVals)
	fmt.Println("this is allExp ", allExp)
	for i := 0; i < len(allKeys); i++ {
		storage[allKeys[i]] = allVals[i] // set storage
		if allExp[i] != "" {
			fmt.Println("have made it inside the expiry handler ")
			ms, _ := strconv.Atoi(allExp[i])
			expired[allKeys[i]] = ms
			// go waitKey(allKeys[i], ms)
		}
	}
	// executing everything in rdb file
	_, exists := configs["manifest"]
	if exists {
		info, _ = os.ReadFile(configs["manifest"])
		fmt.Println(string(info))
		fullPathArr := []string{}
		stringArr := []string{}
		message := []string{}

		fullVal := ""
		fullDir := filepath.Dir(configs["manifest"])

		allPaths := strings.Split(string(info), " ")
		for i := 0; i < len(allPaths); i++ {
			if allPaths[i] == "file" {
				fullPathArr = append(fullPathArr, filepath.Join(fullDir, allPaths[i+1]))
			}
		}
		for _, value := range fullPathArr {
			result, _ := os.ReadFile(value)
			fullVal += string(result)
			fmt.Println("this is what is inside each file ", stringArr) // for each file may have more than one comamnd
		}

		reader := bufio.NewReader(bytes.NewReader([]byte(fullVal))) // bufio needs access to some type of dynamic reader, otherwise byte arr is just statically sitting in memory
		for message != nil {
			message, _ = parser(reader)
			if message != nil {
				execute(message, conn, fullPort, &userAuth)
			}
		}

	}

	for {
		input := ""
		statement, recreatedCmd := parser(reader) // if nil break
		if statement == nil {
			fmt.Println("something happened when parsing and connection disconnected")
			break
		}
		_, exists := configs["manifest"]
		if exists {
			if slices.Contains(writeStatements, statement[0]) {
				// result, _ := os.ReadFile(configs["manifest"]) //  use manifest to identify
				fmt.Println("made it to the append stage ")
				fullPath := filepath.Join(configs["dir"], configs["appenddirname"])
				targetFile := filepath.Join(fullPath, fmt.Sprintf("%s.1.incr.aof", configs["appendfilename"])) // find targetFile string
				fmt.Println("this is target file ", targetFile)

				file, _ := os.OpenFile(targetFile, os.O_APPEND|os.O_WRONLY, 0644)
				file.WriteString(recreatedCmd)
				file.Close()
				//write the cmd to target file
			}
		}
		if len(statement) != 0 {
			input = strings.ToUpper(statement[0])
		}
		// manage masterUpdate by checking when doesn't equal one of those.
		fmt.Println("before going into check is ", masterUpdate)
		//______________________________ auth mode _________________________________________________
		fmt.Println("userAuth is after setting ", userAuth)
		if !authState && !userAuth && input != "AUTH" {
			fmt.Println("made it inside the authenticatino error ")
			conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
			continue
		}
		//___________________________master mode propogation ___________________________________________
		if masterUpdate == true && data["role"] == "master" { //after three way connection
			fmt.Println("propogating down to slave here's statement ", statement)
			if slices.Contains(writeStatements, input) {
				curr_offset, _ := strconv.Atoi(data["master_repl_offset"]) // track master offset
				new_offset := curr_offset + len(recreatedCmd)
				data["master_repl_offset"] = strconv.Itoa(new_offset)
				for conn, _ := range slaveConnections {
					message := createArr(statement, 0, len(statement))
					fmt.Println("for propogation message is ", message)
					conn.Write([]byte(message))

				}
			}

		}
		//____________________________ subscribe mode ________________________________________
		if subscribeMode && !slices.Contains(subStatements, input) {
			if input == "PING" {
				conn.Write([]byte("*2\r\n$4\r\npong\r\n$0\r\n\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("-ERR can't execute '%s' when one or more subscriptions exist\r\n", input)))
			}
			continue
		}

		if input == "MULTI" && isQueue == false { //set queue as long as no tin queue
			// but length is bad, then or isQueue = false
			isQueue = true
			conn.Write([]byte("+OK\r\n"))
		} else if input == "XADD" {
			//  redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95 "1526919030474-0"
			stream_key := statement[1]
			stream_id := statement[2]

			_, exists := streams[stream_key]
			if !exists { // it's a key value, then key value, intiialized as empty, for lists we use {}
				streams[stream_key] = map[string]map[string]string{}
				prev_id = "0-0"
			}
			_, idThere := streams[stream_key][stream_id] // if current id is
			if !idThere {
				streams[stream_key][stream_id] = map[string]string{}
			}
			real_incr := 0
			ms := 0
			prevms := 0
			previncr := 0

			if stream_id == "*" {
				ms = int(time.Now().UnixMilli())
				fmt.Println("made it inside * and ms is ", ms)

				for key := range streams[stream_key] {
					fmt.Println("made it inside for loop ", key, streams[stream_key])
					if key == strconv.Itoa(ms) {
						temp_incr, _ := strconv.Atoi(strings.Split(key, "-")[1])
						real_incr = temp_incr + 1
					}
				}
			} else {
				fmt.Println("made it inside the second step hwere not *")
				ms, _ = strconv.Atoi(strings.Split(stream_id, "-")[0])
				incr := strings.Split(stream_id, "-")[1]

				prevms, _ = strconv.Atoi(strings.Split(prev_id, "-")[0])
				previncr, _ = strconv.Atoi(strings.Split(prev_id, "-")[1])

				if incr == "*" {
					fmt.Println("made it inside the incr step")
					if ms == prevms {
						real_incr = previncr + 1
					} else {
						real_incr = 0
					}
				} else {
					real_incr, _ = strconv.Atoi(incr)
				}
			}
			fmt.Println(ms)

			if ms == 0 && real_incr == 0 {
				conn.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
				continue
			} else if ms < prevms || (ms == prevms && real_incr <= previncr) {
				conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
				continue
			}

			for i := 3; i < len(statement); i += 2 {
				fmt.Println("this is the streams ", streams)
				key := statement[i]
				value := statement[i+1]
				fmt.Println("this is key and value ", key, value)
				fmt.Println("this is id ", stream_id)
				streams[stream_key][stream_id][key] = value
			}
			prev_id = fmt.Sprintf("%d-%d", ms, real_incr)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(prev_id), prev_id)))
		} else if input == "WATCH" { // set keys that can't be changed
			if isQueue == true {
				conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
			} else {
				for i := 1; i < len(statement); i++ {
					watchedKeys[statement[i]] = storage[statement[i]]
				}
				conn.Write([]byte("+OK\r\n"))
			}

		} else if input == "UNWATCH" { // reset, remove all watched keys
			watchedKeys = make(map[string]string)
			watchCheck = false
			conn.Write([]byte("+OK\r\n"))

		} else if input == "DISCARD" { // remove all queued items reset
			if isQueue == true {
				isQueue = false
				queue = [][]string{}
				watchedKeys = make(map[string]string)
				watchCheck = false
				conn.Write([]byte("+OK\r\n"))
			} else {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			}
		} else if input == "EXEC" { //if queued items then start executing them
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
						writeVal := execute(queue[i], conn, fullPort, &userAuth)
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

		} else if strings.Split(input, " ")[0] == "FULLRESYNC" { // second step of 3 way handshake for master
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
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data)))                    // send RDB file
		} else if input == "SUBSCRIBE" {
			subscribeMode = true
			numChannels := 0
			channel := statement[1]
			totalSubs[channel] = append(totalSubs[channel], conn)

			fmt.Println("this is our channel array ", channelArr)
			if slices.Contains(channelArr, channel) {
				numChannels = len(channelArr)
			} else {
				fmt.Println("was not found in channel so updated ")
				channelArr = append(channelArr, channel)
				numChannels = len(channelArr)
				fmt.Println("channel updated ? ", channelArr)
			}
			conn.Write([]byte(fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n", len("subscribe"), "subscribe", len(channel), channel, numChannels)))
		} else if input == "UNSUBSCRIBE" {
			//pop channel arr as well
			channelName := statement[1]
			if slices.Contains(totalSubs[channelName], conn) {
				//go through channel arr, and sub arr pop both
				for j := 0; j < len(channelArr); j++ {
					if channelArr[j] == channelName {
						channelArr = append(channelArr[:j], channelArr[j+1:]...) // remove from local connections
					}
				}
				for i := 0; i < len(totalSubs[channelName]); i++ {
					if totalSubs[channelName][i] == conn {
						totalSubs[channelName] = append(totalSubs[channelName][:i], totalSubs[channelName][i+1:]...) // remove from global connections
					}
				}
			}
			conn.Write([]byte(fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channelName), channelName, len(channelArr))))

		} else if isQueue == true && len(statement) > 0 { // to actually put stuff inside our queue

			queue = append(queue, statement)
			conn.Write([]byte("+QUEUED\r\n"))

		} else {
			if input == "" { // means nothing was sent in command, or smth happened along the way
				continue
			} else {
				writeVal := execute(statement, conn, fullPort, &userAuth)
				// we've created manifest file + appenddirname + appendfilename
				if masterUpdate && data["role"] == "slave" { //in case of slave + needing to update offset
					curr_offset, _ := strconv.Atoi(data["master_repl_offset"])
					new_offset := curr_offset + len(recreatedCmd)

					fmt.Println("made it inside to the update offset ", curr_offset)
					data["master_repl_offset"] = strconv.Itoa(new_offset)
				}
				if writeVal != "" {
					conn.Write([]byte(writeVal))
				}
			}
		}

	}
}

// ______________________________ reading command __________________________________________
func execute(statement []string, conn net.Conn, fullPort string, userAuth *bool) string {
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
		fmt.Println("this is storage: ", storage)
		fmt.Println("these are slave connections: ", slaveConnections)
		storageKey := statement[1]
		ms, _ := expired[storageKey]
		if ms != 0 {
			expiryTime := time.UnixMilli(int64(ms))
			if time.Now().After(expiryTime) {
				delete(storage, storageKey)
				delete(expired, storageKey)
			}
		} // check if assigned expiry is there to delete

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
		if !exists {
			return "*0\r\n"
		}
		start, _ := strconv.Atoi(statement[2])
		stop, _ := strconv.Atoi(statement[3])
		return findRange(lists[listName], start, stop)
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
		if len(statement) > 2 && statement[1] == "GETACK" {
			fmt.Println("made it to GETACK")
			offset := data["master_repl_offset"]
			return fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offset), offset)
		} else if len(statement) > 2 && statement[1] == "ACK" {
			fmt.Println("Recieved ACK statement ", statement[2])
			slaveConnections[conn]["offset"] = statement[2]
			return ""
		}
		return "+OK\r\n"
	case "WAIT":
		if data["role"] == "master" {

			// either after time is expired, or if completed before
			for connection, _ := range slaveConnections {
				connection.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")) // continiously sends this out every ticker second, and if received, will
			}
			target, _ := strconv.Atoi(statement[1])
			sleep, _ := strconv.Atoi(statement[2])
			ch := make(chan string)
			go waitOnConnections(sleep, target, ch)
			return <-ch
		}
		return ""
	case "CONFIG":
		files := statement[2]
		directory := configs["dir"]
		filePath := configs["dbfilename"]
		fmt.Println("this is dir ", directory)

		switch files {
		case "dir":
			return fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(directory), directory)
		case "dbfilename":
			return fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(filePath), filePath)

		case "appendonly":
			return fmt.Sprintf("*2\r\n$10\r\nappendonly\r\n$%d\r\n%s\r\n", len(configs["appendonly"]), configs["appendonly"]) // appendonly, no
		case "appenddirname":
			return fmt.Sprintf("*2\r\n$13\r\nappenddirname\r\n$%d\r\n%s\r\n", len(configs["appenddirname"]), configs["appenddirname"]) // appenddirname, appendonlydir

		case "appendfilename":
			return fmt.Sprintf("*2\r\n$14\r\nappendfilename\r\n$%d\r\n%s\r\n", len(configs["appendfilename"]), configs["appendfilename"]) // appendfilename, appendonly.aof
		case "appendfsync":
			return fmt.Sprintf("*2\r\n$11\r\nappendfsync\r\n$%d\r\n%s\r\n", len(configs["appendfsync"]), configs["appendfsync"]) // everysec
		default:
			return ""
		}
	case "KEYS":
		fmt.Println("made it inside Keys ", configs["dbfilename"])
		directory := configs["dir"]
		filePath := configs["dbfilename"]
		fullPath := filepath.Join(directory, filePath)
		info, _ := os.ReadFile(fullPath) //create byte arr
		fmt.Println("this is supposed to be RDB ", info)
		if info == nil {
			return ""
		}
		allKeys, _, _ := readRDB(info)
		decide := statement[1]
		switch decide {
		case "*":
			fmt.Println("here are all the keys ", allKeys)
			message := createArr(allKeys, 0, len(allKeys))
			return message
		}
		return ""
	case "PUBLISH":
		channelName := statement[1]
		message := statement[2]
		for _, conn := range totalSubs[channelName] {
			conn.Write([]byte(fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channelName), channelName, len(message), message)))
		}
		return fmt.Sprintf(":%d\r\n", len(totalSubs[channelName]))
	case "ZADD":
		setName := statement[1]
		setScore, _ := strconv.ParseFloat(statement[2], 64)
		firstName := statement[3]
		length := len(sortedSets[setName])
		e := Entry{Member: firstName, Score: setScore}
		count := 1

		for j := 0; j < length; j++ {
			if sortedSets[setName][j].Member == e.Member { // pop it out only if exists
				sortedSets[setName] = append(sortedSets[setName][:j], sortedSets[setName][j+1:]...)
				count = 0
				break
			}
		} // in case of nothing will handle that, in case exists, will isolate it, in case of multiple have to loop
		// in case of delete can handle that inside other

		insertArr := sortEntries(sortedSets[setName], e)
		fmt.Println("this is insertARr ", insertArr)
		sortedSets[setName] = insertArr

		return fmt.Sprintf(":%d\r\n", count)
	case "ZRANK":
		setName := statement[1]
		firstName := statement[2]
		for i, entries := range sortedSets[setName] {
			if entries.Member == firstName {
				rank := i
				return fmt.Sprintf(":%d\r\n", rank)
			}
		}
		return "$-1\r\n"
	case "ZRANGE":
		setName := statement[1]
		_, exists := sortedSets[setName]
		if !exists {
			return "*0\r\n"
		}
		validArr := []string{}
		for _, entries := range sortedSets[setName] {
			validArr = append(validArr, entries.Member)
		}

		start, _ := strconv.Atoi(statement[2])
		stop, _ := strconv.Atoi(statement[3])
		return findRange(validArr, start, stop)
	case "ZCARD":
		setName := statement[1]
		_, exists := sortedSets[setName]
		if !exists {
			return ":0\r\n"
		}
		return fmt.Sprintf(":%d\r\n", len(sortedSets[setName]))
	case "ZSCORE":
		setName := statement[1]
		member := statement[2]
		for _, entries := range sortedSets[setName] {
			if entries.Member == member {
				a := strconv.FormatFloat(entries.Score, 'f', -1, 64)
				return fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
			}
		}
		return "$-1\r\n "
	case "ZREM":
		setName := statement[1]
		member := statement[2]
		for j, entries := range sortedSets[setName] {
			if member == entries.Member {
				sortedSets[setName] = append(sortedSets[setName][:j], sortedSets[setName][j+1:]...)
				return ":1\r\n"
			}
		}
		return ":0\r\n"
	case "GEOADD":
		setName := statement[1]
		memberName := statement[4]
		longitude, _ := strconv.ParseFloat(statement[2], 64)
		latitude, _ := strconv.ParseFloat(statement[3], 64)
		if !(longitude >= -180 && longitude <= 180) || !(latitude >= -85.05112878 && latitude <= 85.05112878) {
			return fmt.Sprintf("-ERR invalid longitude,latitude pair %f, %f\r\n", longitude, latitude)
		}
		score := calcGeoScore(latitude, longitude)

		e := Entry{Member: memberName, Score: float64(score)}
		sortedSets[setName] = append(sortedSets[setName], e)
		return ":1\r\n"
	case "GEOPOS":
		message := fmt.Sprintf("*%d\r\n", len(statement)-2)
		fullList, _ := sortedSets[statement[1]]

		for i := 2; i < len(statement); i++ {
			found := false
			for _, entries := range fullList {
				if entries.Member == statement[i] {
					latitude, longitude := reverseGeoScore(int(entries.Score))
					fmt.Println("this is latitude ", latitude)
					x := strconv.FormatFloat(latitude, 'f', -1, 64)
					y := strconv.FormatFloat(longitude, 'f', -1, 64)

					message += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(y), y, len(x), x)
					found = true
					break
				}
			}
			if !found {
				message += "*-1\r\n"
			}
		}
		return message
	case "GEODIST":
		setName := statement[1]
		place1 := statement[2]
		place2 := statement[3]
		var x1, y1, x2, y2 float64
		for _, entries := range sortedSets[setName] {
			if entries.Member == place1 {
				x1, y1 = reverseGeoScore(int(entries.Score))
			}
			if entries.Member == place2 {
				x2, y2 = reverseGeoScore(int(entries.Score))
			}
		}
		distance := hsDist(x1, x2, y1, y2)
		result := strconv.FormatFloat(distance, 'f', -1, 64)
		return fmt.Sprintf("$%d\r\n%s\r\n", len(result), result)
	case "GEOSEARCH":
		validPlaces := []string{}
		setName := statement[1]
		long1 := 0.0
		lat1 := 0.0
		radius := 0.0

		for i := 1; i < len(statement); i++ {
			switch statement[i] {
			case "FROMLONLAT":
				long1, _ = strconv.ParseFloat(statement[i+1], 64)
				lat1, _ = strconv.ParseFloat(statement[i+2], 64)
			case "BYRADIUS":
				radius, _ = strconv.ParseFloat(statement[i+1], 64)
			}
		}

		for _, entries := range sortedSets[setName] {
			lat2, long2 := reverseGeoScore(int(entries.Score))
			distance := hsDist(lat1, lat2, long1, long2)
			if distance <= radius {
				validPlaces = append(validPlaces, entries.Member)
			}
		}
		return createArr(validPlaces, 0, len(validPlaces))
	case "ACL":
		switch statement[1] {
		case "WHOAMI": // have to find connection and then find user
			user := "default"
			for name, u := range users {
				if u.Connection == conn {
					user = name
				}
			}
			if slices.Contains(users[user].Flags, "nopass") && authState {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(user), user)
			} else if *userAuth { // handle auth logic here
				return fmt.Sprintf("$%d\r\n%s\r\n", len(user), user)
			} else {
				return "-NOAUTH Authentication required.\r\n"
			}

		case "GETUSER":
			user := statement[2]
			p := users[user].Passwords
			f := users[user].Flags // get passwords and flags
			flags := createArr(f, 0, len(f))
			passwords := createArr(p, 0, len(p))
			return fmt.Sprintf("*4\r\n$5\r\nflags\r\n%s$9\r\npasswords\r\n%s", flags, passwords)

		case "SETUSER":
			user := statement[2]
			flags := []string{"nopass"}
			passwords := []string{}
			password := ""

			if len(statement) > 3 { // if password exists
				flags = []string{}
				password = statement[3][1:]
				authState = false
				*userAuth = true
			}
			hashedPassword := sha256.Sum256([]byte(password)) // gives hashed password in 32 bits
			hashPass := fmt.Sprintf("%x", hashedPassword)     // gives hash password in hexdecimal

			_, exists := users[user] // check if user exists

			if exists {
				upUser := User{Connection: conn, Passwords: append(users[user].Passwords, hashPass)}
				if len(users[user].Passwords) == 1 { // this means just added
					upUser = User{Connection: conn, Passwords: append(users[user].Passwords, hashPass), Flags: []string{}}
				}
				fmt.Println("the user is ", upUser)
				users[user] = upUser
				fmt.Println("the user is now ", users[user])
			} else {
				passwords = []string{hashPass}
				newUser := User{Connection: conn, Passwords: passwords, Flags: flags}
				users[user] = newUser
			}
			return "+OK\r\n"
		default:
			return ""
		}
	case "AUTH":
		user := statement[1]
		password := statement[2]
		hashedPassword := sha256.Sum256([]byte(password)) // gives hashed password in 32 bits
		hashPass := fmt.Sprintf("%x", hashedPassword)     // gives hash password in hexdecimal
		if len(users[user].Passwords) == 0 {
			newUser := User{Connection: conn, Passwords: []string{hashPass}}
			users[user] = newUser
			*userAuth = true
			authState = false
			return "+OK\r\n"
		} else if slices.Contains(users[user].Passwords, hashPass) {
			*userAuth = true
			return "+OK\r\n"
		}
		return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
	case "TYPE":
		//string, list, set, zset, hash, stream, and vectorset
		sk := statement[1]
		_, storageExists := storage[sk]
		_, streamExists := streams[sk] // problem is the keys could be the same
		if storageExists {
			return "+string\r\n"
		} else if streamExists {
			return "+stream\r\n"
		}
		return "+none\r\n"
	case "XRANGE":
		key := statement[1] 
		arg1 := strings.Split(statement[2], "-")
		arg2 := strings.Split(statement[3], "-")
		ms1,_ := strconv.Atoi(arg1[0]) //  first milliseconds
		ms2,_ := strconv.Atoi(arg2[0]) // second milliseconds
		incr := 0
		incr2 := math.MaxInt32

		if len(arg1) > 1{
			incr,_ = strconv.Atoi(arg1[1])
		}
		if len(arg2) > 1{
			incr2,_ = strconv.Atoi(arg2[1])
		}

		fmt.Println("all args ", incr, incr2, ms1, ms2)

		// ms == ms this is valid, but make sure everything is greater than or equal to index
		// >ms <ms2 is all valid
		// == ms2 make sure less than or equal to index if specified
		count := 0 
		goodMessage := ""

		for data, value := range streams[key]{ // this goes through all our id's 
			msKey,_ := strconv.Atoi(strings.Split(data, "-")[0])
			incrKey,_ := strconv.Atoi(strings.Split(data, "-")[1])
			if statement[2] == "-" {
				if msKey < ms2 || (incrKey <= incr2 && msKey == ms2){
					goodMessage += createChunk(data, value)
					count++
				}
			}else if statement[3] == "+"{
				if msKey > ms1 ||( incrKey >= incr && msKey == ms1){
					goodMessage += createChunk(data, value)
					fmt.Println("this is good message growing ", goodMessage)
					count++
				}
			}else{
				if ms1 == msKey && incrKey >= incr{// have to go inside and get all kv pairs
					goodMessage += createChunk(data, value)
					count++
				}else if msKey > ms1 && msKey < ms2{
					goodMessage += createChunk(data, value)
					count ++ 
					//do something with data
				}else if msKey == ms2 && incrKey < incr2 && ms2>ms1{
					goodMessage += createChunk(data,value)
					count ++ 
				}
			}
		}		
		preMessage := fmt.Sprintf("*%d\r\n", count)
		preMessage += goodMessage
		return preMessage
	case "XREAD": 	
		startind := 0
		milliseconds := 0
		for i, vals := range statement{
			if vals == "BLOCKING"{
				milliseconds,_ = strconv.Atoi(statement[i+1])
			}else if strings.ToUpper(vals) == "STREAMS"{
				startind = i+1
			}
		} 

		length := (len(statement)-startind)/2
		keys := statement[startind: startind+length] 
		idBound := statement[startind + length:]
		fmt.Println("this is keys ", keys)
		fmt.Println("this is idBound ", idBound)
 

		message, count := xread(keys, idBound)
		if count == 0 {
			ch := make(chan string)
			go waitXread(milliseconds, ch, keys, idBound)
			return <-ch

		}else{
			return message
		}

		// have some check for entries, and then if none, create channel
		// call go func, and set a timer if, select case
		// if channel responds with whatever, then we pass that it into the rest of this 
		// that means create function of this, case that return func, otherwise null, else this func
		// takes in keys + indicies already specifie

	default:
		return ("+messageNotFound\r\n")
	}
} // so if equal then run a loop that goes through all that are equal and sort of lexigraphically

func waitXread(ms int, ch chan string, keys []string, idBound []string){
	ticker := time.NewTicker(time.Duration(10) * time.Millisecond)
	deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)

	for range ticker.C{
		message, count := xread(keys, idBound)
		if(count > 0){
			ch <- message
		}else if time.Now().After(deadline){
			ch <- "*-1\r\n"
		}	
	}
}


func xread(keys []string, idBound []string)(string, int){
	fullStr := ""
	countKeys := 0
	totalEntries := 0
	
	for i:=0; i < len(keys); i++ {  
		count:=0
		kv := ""

		id := strings.Split(idBound[i], "-")
		ms1,_ := strconv.Atoi(id[0]) // first milliseconds
		incr,_ := strconv.Atoi(id[1]) // get id associated with it. 
		countKeys ++ 

		fmt.Println("this is my id and ms for each ", ms1, incr)
		// list of ids, in each id 
		for ids, vals := range streams[keys[i]]{
			msKey,_ := strconv.Atoi(strings.Split(ids, "-")[0])
			incrKey,_ := strconv.Atoi(strings.Split(ids, "-")[1])
			if msKey > ms1 ||( msKey == ms1 && incrKey >= incr){
					kv += createChunk(ids, vals) 
					count ++
					totalEntries ++
			}
		}
		insideArr := fmt.Sprintf("*%d\r\n", count) + kv
		fullStr += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n%s", len(keys[i]), keys[i], insideArr)

	}
	return fmt.Sprintf("*%d\r\n", countKeys) + fullStr, totalEntries
}


func createChunk(data string, value map[string]string)string{
	tempArr := []string{}
	fmt.Print("made it inside create chunk  ", value)
	for key1,values := range value{ 
		tempArr = append(tempArr, key1, values)  //array of strings, back to back to back
	}
	message := createArr(tempArr, 0, len(tempArr))
	other := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n%s", len(data), data, message)
	return other
}

func hsDist(lat1 float64, lat2 float64, long1 float64, long2 float64) float64 {
	const rEarth = 6372797.560856
	lat1 = lat1 * math.Pi / 180
	lat2 = lat2 * math.Pi / 180
	long1 = long1 * math.Pi / 180
	long2 = long2 * math.Pi / 180

	return 2 * rEarth * math.Asin(math.Sqrt(haversine(lat2-lat1)+
		math.Cos(lat1)*math.Cos(lat2)*haversine(long2-long1)))
}
func haversine(θ float64) float64 {
	return .5 * (1 - math.Cos(θ))
}

func calcGeoScore(x float64, y float64) int {
	MIN_X := -85.05112878
	MAX_X := 85.05112878
	MIN_Y := -180.00
	MAX_Y := 180.00

	LAT_RANGE := MAX_X - MIN_X
	LONG_RANGE := MAX_Y - MIN_Y

	x = ((x - MIN_X) / LAT_RANGE) * math.Pow(2, 26)
	y = ((y - MIN_Y) / LONG_RANGE) * math.Pow(2, 26)

	norm_x := int(x) // this produces loss, rounding to int removes precission
	norm_y := int(y)

	norm_x = shiftedVals(norm_x)
	norm_y = shiftedVals(norm_y)

	interleaved_val := norm_x | (norm_y << 1)
	return interleaved_val

}
func shiftedVals(num int) int { //splitting bits by 0 such that are 0s between everything
	num = num & 0xFFFFFFFF                     //makes sure lower 32 bits are 0
	num = (num | num<<16) & 0x0000FFFF0000FFFF // taking half of bits moving them up then cutting out the previous top.
	num = (num | num<<8) & 0x00FF00FF00FF00FF
	num = (num | num<<4) & 0x0F0F0F0F0F0F0F0F
	num = (num | num<<2) & 0x3333333333333333
	num = (num | num<<1) & 0x5555555555555555

	return num
}

func reverseGeoScore(geocode int) (float64, float64) {
	fmt.Println("made it inside reverse Geoscore ", geocode)
	y := geocode >> 1
	x := geocode

	new_x := shiftBackVals(x)
	new_y := shiftBackVals(y)

	MIN_X := -85.05112878
	MAX_X := 85.05112878
	MIN_Y := -180.00
	MAX_Y := 180.00

	LAT_RANGE := MAX_X - MIN_X
	LONG_RANGE := MAX_Y - MIN_Y
	fmt.Println("ranges are ", LAT_RANGE, LONG_RANGE)

	convx := float64(new_x)
	convy := float64(new_y)
	fmt.Println("this is new _x ", new_x)
	fmt.Println("this is new_y ", new_y)
	fmt.Println("this is convx ", convx)
	fmt.Println("this is convy ", convy)

	fmt.Println("first part ", convx/math.Pow(2, 26))
	fmt.Println("second part ", convx/math.Pow(2, 26)*LAT_RANGE)
	fmt.Println("third part ", (convx/math.Pow(2, 26)*LAT_RANGE)+MIN_X)

	//idea is that we qunatize a number line, and therefore the converted number is not
	x_edge := (LAT_RANGE * (convx / math.Pow(2, 26))) + MIN_X             // convert to float and redo the math before
	x_other_edge := (LAT_RANGE * ((convx + 1) / math.Pow(2, 26))) + MIN_X // convert to float and redo the math before
	y_edge := (LONG_RANGE * (convy / math.Pow(2, 26))) + MIN_Y
	y_other_edge := (LONG_RANGE * ((convy + 1) / math.Pow(2, 26))) + MIN_Y

	fmt.Println("this is the x stuff ", x_edge, x_other_edge)
	final_x := (x_edge + x_other_edge) / 2
	final_y := (y_edge + y_other_edge) / 2
	return final_x, final_y
}

func shiftBackVals(num int) int {
	num = num & 0x5555555555555555
	num = (num | (num >> 1)) & 0x3333333333333333 //shifting back + undoing mask
	num = (num | (num >> 2)) & 0x0F0F0F0F0F0F0F0F
	num = (num | (num >> 4)) & 0x00FF00FF00FF00FF
	num = (num | (num >> 8)) & 0x0000FFFF0000FFFF
	num = (num | (num >> 16)) & 0x00000000FFFFFFFF
	return num
}

func sortEntries(arr []Entry, e Entry) []Entry {
	fmt.Println("this is the arr before sorting ", arr)
	for i := 0; i < len(arr); i++ {
		curr := arr[i]                                                                 // less than catches case that e.Member > than all others. everything else caught by return
		if e.Score < curr.Score || (e.Score == curr.Score && e.Member < curr.Member) { //  so if equal then check if less, insert otherwise wait until end then go
			return slices.Insert(arr, i, e)
		}
	}
	return append(arr, e)
}
func findRange(arr []string, start int, stop int) string {
	length := len(arr)
	if stop > length-1 {
		stop = length - 1
	}
	if start < 0 { // clamp 0  and handle negative
		start = max(length+start, 0)
	}
	if stop < 0 { // clamp 0  and handle negative
		stop = max(length+stop, 0)
	}
	if start >= length || start > stop {
		return ("*0\r\n")
	}
	message := createArr(arr, start, stop+1)
	return message
}

func readRDB(info []byte) ([]string, []string, []string) {
	fmt.Println("made it inside RDB check ")

	i := 0
	count := 0

	allKeys := []string{}
	allVals := []string{}
	allExp := []string{}

	//_________________________ have to still handle FD and convert to seconds ______________________
	for i < len(info) {
		if info[i] == 0xFF{
			break
		}
		if info[i] == 0xFB {
			length := int(info[i+1])
			// fmt.Println("this is the length given by oxfb ", length)
			allExp = make([]string, length)
			i = i + 3
			for j := 0; j < length; j++ {
				// fmt.Println("this is where we are ", i, len(info))
				if info[i] == 0xFC { // at this point at 0xFC
					// unix timestamp uses little endian, so backwards by size of bytes.
					tempExp := binary.LittleEndian.Uint64(info[i+1 : i+9])
					expiry := strconv.FormatUint(tempExp, 10)
					allExp[count] = expiry
					i = i + 9
				} // at this point at 0x00
				fmt.Println("this is supposed to be before kv ", info[i])
				realLen := int(info[i+1])
				keyLen := i + 2 + realLen
				keys := info[i+2 : keyLen]

				realLen2 := int(info[keyLen])
				vals := info[keyLen+1 : keyLen+1+realLen2]

				// fmt.Println("this is data ", string(keys))
				// fmt.Println("this is value ", string(vals))

				allKeys = append(allKeys, string(keys))
				allVals = append(allVals, string(vals))
				count++
				i = i + 3 + realLen + realLen2
			}
		} else {
			i++
		}
	}

	// fmt.Println("this is all Keys ", allKeys)
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

func wait(key string, ms int) {
	deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(10) * (time.Millisecond))
	fmt.Println(" here are the keys to be deleted ", key)

	for range ticker.C {
		if time.Now().After(deadline) {
			delete(storage, key)
			// ticker.Stop() // set ticker that when first time runs out, just delete, and then go on.
		}
	}
}
func parser(reader *bufio.Reader) ([]string, string) {
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
		r, _ := reader.ReadString('$')                // by pass the /r/n and $
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
			return nil, ""
		}
		fmt.Println("expectingRDB inside parse is ", expectingRDB)
		if expectingRDB { //handle RDB file
			fmt.Println("this is initVal ", initVal)
			buf := make([]byte, initVal) // we set a buffer
			io.ReadFull(reader, buf)     // consume and discard
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

	e, _ := reader.ReadString('\n')
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
	fmt.Println("immediately after parsing cmd is ", recreatedCmd)
	fmt.Println("immediately after parsing statement is ", statement)
	return statement, recreatedCmd
}
func writeUpdate(returnVal string) string {
	if data["role"] == "master" {
		return returnVal
	} else {
		fmt.Println("made it here as a slave to writeUpdate")
		return ""
	}
}
func waitOnConnections(sleep int, target int, ch chan string) {
	deadline := time.Now().Add(time.Duration(sleep) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(10) * time.Millisecond)
	count := 0
	masterOffset, _ := strconv.Atoi(data["master_repl_offset"])

	// need to send REPLGEETACK

	for range ticker.C {
		fmt.Println("this is masterOffset", masterOffset)
		if time.Now().After(deadline) {
			fmt.Println("made it to the point where it overextended")
			ch <- fmt.Sprintf(":%d\r\n", count) // go into infinite for loop wait until after deadline
			ticker.Stop()
			break
		} else { // keep resetting such can count from fresh.
			count = 0
			for conn, _ := range slaveConnections {
				offsetVal, _ := strconv.Atoi(slaveConnections[conn]["offset"])
				fmt.Println("this is Slave offset ", offsetVal)
				if offsetVal >= masterOffset {
					count++
				}
			}
			if count >= target {
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
	fmt.Println("this is the current direcotry ", curr_dir)
	configs["dir"] = curr_dir
	configs["appendonly"] = "no"
	configs["appenddirname"] = "appendonlydir"
	configs["appendfilename"] = "appendonly.aof"
	configs["appendfsync"] = "everysec"

	if len(os.Args) > 2 {
		if os.Args[1] == "--port" || os.Args[1] == "-p" {
			fullPort = os.Args[2]
		} else if os.Args[1] == "--dir" {
			fmt.Println("made it inside the dir function check ")
			for i := 0; i < len(os.Args); i++ {
				if os.Args[i] == "--dir" {
					configs["dir"] = os.Args[i+1]
				} else if os.Args[i] == "--dbfilename" {
					configs["dbfilename"] = os.Args[i+1]
				} else if os.Args[i] == "--appendonly" {
					configs["appendonly"] = os.Args[i+1]
				} else if os.Args[i] == "--appenddirname" {
					configs["appenddirname"] = os.Args[i+1]
				} else if os.Args[i] == "--appendfilename" {
					configs["appendfilename"] = os.Args[i+1]
				} else if os.Args[i] == "--appendfsync" {
					configs["appendfsync"] = os.Args[i+1]
				}
			}
			if configs["appendonly"] == "yes" {
				fullPath := filepath.Join(configs["dir"], configs["appenddirname"])
				fileArr, _ := os.ReadDir(fullPath)
				incrCount := 0
				manifestFile := filepath.Join(fullPath, fmt.Sprintf("%s.manifest", configs["appendfilename"]))

				if len(fileArr) > 0 {
					for i := 0; i < len(fileArr); i++ {
						fileParts := strings.Split(fileArr[i].Name(), ".")
						if slices.Contains(fileParts, "incr") {
							incrCount++
						}
						if slices.Contains(fileParts, "manifest") {
							manifestFile = filepath.Join(fullPath, fileArr[i].Name())
						}
					}
				} else {
					os.MkdirAll(fullPath, 0755) //create a directory 0755 is just permission logic
				}

				filePath := filepath.Join(fullPath, fmt.Sprintf("%s.%d.incr.aof", configs["appendfilename"], incrCount+1))
				manifestMessage := fmt.Sprintf("file %s.%d.incr.aof seq %d type i", configs["appendfilename"], incrCount+1, incrCount+1) // type incremental file

				file, _ := os.Create(filePath) // this is our aof, creates on file path
				file.Close()

				instructionFile, _ := os.OpenFile(manifestFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				instructionFile.WriteString(manifestMessage)
				instructionFile.Close() // to append

				inside, _ := os.ReadFile(manifestFile)
				fmt.Println("inside instruction file is  ", string(inside))
				configs["manifest"] = manifestFile
			}
		}
	}
	// 1. read manifest file by recreating if dirname is yes

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
