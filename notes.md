two ways to create asynchronous blockers
time.Sleep(timeDuration) --> ch := make(chan string)
time.Ticker(timeDuration) --> for range ticker.C()

BLPOP --> when duration is 0 wait indefinitely for response, otherwise add 

Questions:
what does ticker.C mean?

traversing objects
___________________________

var objName = make(map[string]string) --> map[key]val
delete(obj, key)
for key, value range object{

}
obj[] = val



replication and master/slave relationship 
________________________________________

slaves are just replicas of masters specified by some command like /file --port 6380 --replicaof localhost:6379 
                                                                    notice that these are just tags


masters have info about them, some of which includes master replication id meaning, the current history id of
the master. Also master replication offset meaning the data sent to the replciation, and also just the state
so if it is a slave or a master. Each database has this: 

for replciation have to have to establish a connection through a handshake, 
to do this dial first after parsing the host and port: etc. net.Dial("tcp", localhost:6279)

here we can send our handshake if all is well. 



error handling 
____________________________________________

so many edge cases, but conditionals get messy, so they use a 

COMMAND TABLE
(no idea what this is maybe check out later)


tcp connections 
_______________________________________________

when establishing a connection with someone,
1. sending and them establishing on their backend
2. or listening and you accepting and establishing 
 
have to set a buffer to capture response 
buf := make([]byte, 1024) --> slice buffer, allocate one place in memory with 1024 bytes, will be filled
OR
use bufio: bufio.NewReader(conn)


observations 
_____________________________________________

continiously have to make modular to support various different functions
examples :

1. have to take switch case for commands, (PONG, SET, BLPOP)
2. have to make our parser agnostic to all cases (listening for connection/creating connection)

basic TCP setup 


net.Listen("tcp", port)  --> listener (this handles the 3 way connection)
listen.Accept()

when parsing out a string, we use a reader to go through, is there a way to 
split by space

terrible freaking error. 
_________________________________________________________________

so when trying to establish connection with master db as a replica, have to send PINGS + REPLCONF + REPLPSYNC
master sends back PONGS + OK + OK + FULLRESYNC + binary data (snapshot of current db), 

when sending binary data, master closes but CONTINUES TO LISTEN ON PORT and when nothing is sent 
we just continued no and on. This means continued to call execute without any starting value, creating
an infinite loop to the point when INFO cmd is finally sent from a different place, CPU has used so many resources


can store connections
___________________________________________________________________

able to store connections, and use flags + information to determine states
and specify functionality

SYNC by bytes. The replica LITERALLY just stores every cmd as bytes. This is 
done by tracking every cmd, in the form of chars, and finding the length. 
Each char represents a byte, so this gives it. 

The master tracks bytes on their own end. To sync at first, master has 
to send RDB file which is a binary snapshot, and then somehow determines 
sending write commands etc. 


reading cli commands 
______________________________________________________________________

os.Args to parse it





things to change
__________________________________________________________________

1. when parsing out commands, make sure to use a for loop because right now cmds
are kind of just hardcoded on this jawn
2. what is uint8 uint16 etc. 
3. unix times. fuck these. basically represent ms since a certain point in time, so have to 
be able to convert them into something comparable. 
using time.UnixMilli(int64(ms)) assuming they are ms, allows for comparing against time.Now()
4. for expiry, only delete when get is called. 