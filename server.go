//decoding and encoding

package main

import (
	"fmt"
	"os"
	//"errors"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	//"strings"
	"./smallhash"
	"log"
	"math"
	"strconv"
	"time"

)

//chord node type
type ChordNode struct {
	NodeID    int
	IpAddress string
	Port      int
}

type ChordArray struct{
	Self ChordNode 
	Successor ChordNode 
	Predecessor ChordNode 
}

const BITSIZE = 8

//Finger table, should contain a list of chordNode objects
var Finger []ChordNode

//Predecessor
var Predecessor ChordNode

//Successor
var Successor ChordNode

//The node itself
var Self ChordNode

//Address/Port of  an existing node in the ring; specified on the terminal when starting server
var StartingIpAddress string
var StartingPort int

//Config file Params for Node
type ConfigParamsType struct {
	ServerID                   string
	Protocol                   string
	IpAddress                  string
	Port                       int
	PersistentStorageContainer PersistentStorageContainerType
	Methods                    []string
}

//Config file name and location
type PersistentStorageContainerType struct {
	File string
}

//value int triplets
type valuetype struct {
	Content    string
	Size       string
	Created    string
	Modified   string
	Accessed   string
	Permission string
}

//Single Item containing triplet data
type DICT3Item []interface{}

//A List of Triplets (data stored in the node)
type Dict3 []DICT3Item

//Request from client
type Operation struct {
	Method string
	Params DICT3Item
	//Id     int
}

//Response from server
type Get struct {
	Result DICT3Item
	//Id     int
	Error  error
}

// Variable that will store the Configuration Parameters for the node when it starts up.
var NodeParams ConfigParamsType

//data
var dict3 Dict3

//key and relation hash: this corresponds to the key used for node lookups
var krhash []int

//Value of 2^PowerOfBitsInChordRing; such as 2^7 = 128
var keybits int

//RPC
type JRPC int

// shows the content of the finger table
func PRINT_FINGERTABLE() {
	fmt.Println("Finger Table size: ", len(Finger))
	for index := 0; index < len(Finger); index++ {
		fmt.Println("index value for Finger[", index, "] is:", Finger[index])
	}
}

/* This function receives as input the [IpAddress and Port] of a node and returns the hash
 Input: Ip and Port number
Output: hash
*/
func IPHash(ipAndPort string) uint64 {
	return smallhash.ModHash(ipAndPort)
}

/* This function receives as input the [key] or [rel] compound key
   Input: [key] or [rel] compound key
   Output: a hash type (unit64)
*/
func KRHash_Key(key string) uint64 {
	return smallhash.ModHash_4(key)
}
func KRHash_Rel(rel string) uint64 {
	return smallhash.ModHash_4(rel)
}

/*This function is used to calculated hashing results of all data stored in the node
*/
func KR_Hash_All() {

	//clear current array which is used to store hashing values of key and relation
	krhash = krhash[:0]

	for i:=0;i<len(dict3);i++{
		hresult_k := KRHash_Key(dict3[i][0].(string) )
		hresult_r :=  KRHash_Rel(dict3[i][1].(string) )
		hresult := float64(hresult_k) * math.Pow(2., float64(BITSIZE/2)) + float64(hresult_r)
		krhash = append(krhash,int(hresult))
	}
}

// get predecessor of a given node
func (r *JRPC) GET_PREDECESSOR(request *ChordNode, response *ChordNode) error {

	response.IpAddress = Predecessor.IpAddress
	response.Port = Predecessor.Port
	response.NodeID = Predecessor.NodeID
	return nil
}

//get successor of a given node
func (r *JRPC) GET_SUCCESSOR(request *ChordNode, response *ChordNode) error {
	response.IpAddress = Successor.IpAddress
	response.Port = Successor.Port
	response.NodeID = Successor.NodeID
	return nil
}

//when a server/node starts up it needs to join the ring, this is where joining the ring happens!
func JOIN() {

	//Self is a ChordNode type;  Jsontype is a Jsonobject type
	Self.IpAddress = NodeParams.IpAddress
	Self.Port = NodeParams.Port
	Self.NodeID = int(IPHash(Self.IpAddress + ":" + strconv.Itoa(Self.Port)))
	Predecessor = Self
	Successor = Self
	fmt.Printf("Initial StartUp of Node with Address: %s:%d & NodeID: %d \n", Self.IpAddress, Self.Port, Self.NodeID)

	// if the node is the first one in the ring, then the starting IPAddress & Port should be equal to Self.IPAddress and Self.Port
	if StartingIpAddress == Self.IpAddress && StartingPort == Self.Port {

		for i := 0; i < BITSIZE; i++ {
			Finger = append(Finger, Self) //initialize finger table
		}
		KR_Hash_All()
		//for i:=0;i<len(dict3);i++{
			//fmt.Printf("%d\n",krhash[i])
		//}
		fmt.Printf("First Node in the ring with Address: %s:%d & NodeID: %d \n", Self.IpAddress, Self.Port, Self.NodeID)

	} else { //else, there is at least one other node existing in the ring already which has the starting IPAddress and Port, so connect to that node

		for i := 0; i < BITSIZE; i++ {
			Finger = append(Finger, Self) //initialize finger table
		}
		fmt.Printf("Joining the ring with Address: %s:%d & NodeID: %d \n", StartingIpAddress, StartingPort, Self.NodeID)

		client, e := jsonrpc.Dial(NodeParams.Protocol, StartingIpAddress+":"+strconv.Itoa(StartingPort)) //call one existing node in the ring

		if e != nil {
			log.Fatal("dialing", e)
		}
		//fmt.Printf("Joining the ring with Address: %s:%d & NodeID: %d \n", Self.IpAddress, Self.Port, Self.NodeID)
		/* Argument 1: The remote method to call
		   Argument 2: The parameters that will be passed to remote method
		   Argument 3: A pointer to a defined type that will store the response.
		*/
		e = client.Call("JRPC.FIND_SUCCESSOR", Self, &Successor)
		if e != nil {
			log.Fatal("dialing", e)
		}
		fmt.Println("RPC.FindSuccessor() -> Response", Successor)

		client.Close() //each time you call client.call(), don't forget to close the connection each time!

		STABILIZE() // Stabilize() is called each time, a new node (other than the first node) joins the ring.

		//Fix finger tables

		var t ChordNode // the calculated NodeID (given a key) based on FingerTable formula; the value of t may/may not exist; if it doesn't exist, find closest successor; and successor will be responsible for t's data

		var s ChordNode //tempSuccessor
		var S ChordNode //tempSuccessor

		var n ChordNode // tempSelf
		var N ChordNode // tempSelf

		n = Self
		s = Successor
		N = Self
		S = Successor

		var count int
		count = 0

		for i := 0; i < BITSIZE; i++ { //because now I set bits as 8 (8-bit ring). There are (at most) 8 rows/entries in the finger table

			n = N
			s = S

			f := float64(i)
			t.NodeID = (n.NodeID + int(math.Pow(2., f))) % keybits //calculate id for each row
			//fmt.Printf("%d: %d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID)
			//fmt.Printf("%d\n",t.NodeID)

			if i == 0 {
				Finger[i] = s // the first row of finger table is its successor
				continue
			} else {

				for {
					//this part is used to find the successor
					if (t.NodeID > n.NodeID && t.NodeID <= s.NodeID) && (n.NodeID <= s.NodeID) {
						Finger[i] = s
						break
					} else if t.NodeID == n.NodeID {
						Finger[i] = n
						break
					} else if t.NodeID == s.NodeID {
						Finger[i] = s
						break
					} else if n.NodeID > s.NodeID {
						if t.NodeID > n.NodeID && t.NodeID < (s.NodeID+keybits) {
							Finger[i] = s
							break
						} else if t.NodeID < s.NodeID && (t.NodeID+keybits) > n.NodeID {
							Finger[i] = s
							break
						}
					}
					n = s

					if s.NodeID != Self.NodeID {
						client, e = jsonrpc.Dial(NodeParams.Protocol, s.IpAddress+":"+strconv.Itoa(s.Port))
						//fmt.Printf("%s\n",s.IpAddress+":"+strconv.Itoa(s.Port))
						if e != nil {
							log.Fatal("dialing", e)
						}
						e = client.Call("JRPC.GET_SUCCESSOR", t, &s)
						client.Close()
					} else {
						s = Successor
					}
					//fmt.Printf("%d,%d\n",n.NodeID, s.NodeID+keybits)
				}
			}
			count = count + 1
		}

		// fix the finger table
		for {
			//fmt.Printf("%d\n",count)
			N = S

			if N.NodeID == Self.NodeID { //which means we already fixed all the nodes
				break
			}

			client, e = jsonrpc.Dial(NodeParams.Protocol, N.IpAddress+":"+strconv.Itoa(N.Port))

			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.GET_SUCCESSOR", Self, &S)
			client.Close()

			client, e = jsonrpc.Dial(NodeParams.Protocol, N.IpAddress+":"+strconv.Itoa(N.Port))

			if e != nil {
				log.Fatal("dialing", e)
			}
			var chordarray ChordArray
			chordarray.Self = Self
			chordarray.Successor = Successor
			e = client.Call("JRPC.FIX_FINGER", chordarray, &t)
			client.Close()

		}

	}
	//PRINT_FINGERTABLE()
	//fmt.Println("Finger Table entry for index 0  is: ", Finger[0]) //println finger[i]
	fmt.Printf("Predecessor: %s:%d | NodeID: %d \n", Predecessor.IpAddress, Predecessor.Port, Predecessor.NodeID)
	fmt.Printf("Successor:    %s:%d | NodeID: %d \n", Successor.IpAddress, Successor.Port, Successor.NodeID)
}

// Parameter 1: ChordNode object, whose successor you want to find
// Parameter 2: returns a ChordNode object,
func (r *JRPC) FIND_SUCCESSOR(request *ChordNode, response *ChordNode) error {

	if Self.NodeID == Successor.NodeID && Self.NodeID == Predecessor.NodeID {
		response.IpAddress = Successor.IpAddress
		response.Port = Successor.Port
		response.NodeID = Successor.NodeID
		return nil
	}

	if request.NodeID > Self.NodeID && request.NodeID <= Successor.NodeID {
		response.IpAddress = Successor.IpAddress
		response.Port = Successor.Port
		response.NodeID = Successor.NodeID
	} else if request.NodeID == Self.NodeID {
		response.IpAddress = Self.IpAddress
		response.Port = Self.Port
		response.NodeID = Self.NodeID
	} else if Self.NodeID > Successor.NodeID {
		if request.NodeID > Self.NodeID && request.NodeID < (Successor.NodeID+keybits) {
			response.IpAddress = Successor.IpAddress
			response.Port = Successor.Port
			response.NodeID = Successor.NodeID
		} else if request.NodeID < Successor.NodeID && (request.NodeID+keybits) > Self.NodeID {
			response.IpAddress = Successor.IpAddress
			response.Port = Successor.Port
			response.NodeID = Successor.NodeID
		}
	} else if request.NodeID > Predecessor.NodeID && request.NodeID <= Self.NodeID {
		response.IpAddress = Self.IpAddress
		response.Port = Self.Port
		response.NodeID = Self.NodeID
	} else if Predecessor.NodeID > Self.NodeID {
		if request.NodeID > Predecessor.NodeID && request.NodeID < (Self.NodeID+keybits) {
			response.IpAddress = Self.IpAddress
			response.Port = Self.Port
			response.NodeID = Self.NodeID
		} else if request.NodeID < Self.NodeID && (request.NodeID+keybits) > Predecessor.NodeID {
			response.IpAddress = Self.IpAddress
			response.Port = Self.Port
			response.NodeID = Self.NodeID
		}
	}else {

		var Nprime ChordNode
		CLOSEST_PRECEDING_NODE(request, &Nprime)
		//fmt.Printf("%d\n", Nprime.NodeID)

		client, e := jsonrpc.Dial(NodeParams.Protocol, Nprime.IpAddress+":"+strconv.Itoa(Nprime.Port))

		if e != nil {
			log.Fatal("dialing", e)
		}

		e = client.Call("JRPC.FIND_SUCCESSOR", request, &response)
		if e != nil {
			log.Fatal("dialing", e)
		}

		client.Close()
	}

	return nil
}

func CLOSEST_PRECEDING_NODE(request *ChordNode, response *ChordNode) error {

	if request.NodeID < Finger[0].NodeID {
		response.IpAddress = Finger[0].IpAddress
		response.Port = Finger[0].Port
		response.NodeID = Finger[0].NodeID
		return nil
	}

	for i := 0; i < BITSIZE; i++ {
		if Finger[i+1].NodeID >= Finger[i].NodeID {
			if request.NodeID >= Finger[i].NodeID && request.NodeID < Finger[i+1].NodeID {
				response.IpAddress = Finger[i].IpAddress
				response.Port = Finger[i].Port
				response.NodeID = Finger[i].NodeID
				return nil
			}
		} else {
			if request.NodeID < Finger[i+1].NodeID && (request.NodeID+keybits) >= Finger[i].NodeID {
				response.IpAddress = Finger[i].IpAddress
				response.Port = Finger[i].Port
				response.NodeID = Finger[i].NodeID
				return nil
			} else if request.NodeID >= Finger[i].NodeID && request.NodeID < (Finger[i+1].NodeID+keybits) {
				response.IpAddress = Finger[i].IpAddress
				response.Port = Finger[i].Port
				response.NodeID = Finger[i].NodeID
				return nil
			}
		}
	}
	response.NodeID = Self.NodeID
	response.IpAddress = Self.IpAddress
	response.Port = Self.Port
	return nil
}

func STABILIZE() {

	var x ChordNode

	client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
	if e != nil {
		log.Fatal("dialing", e)
	}

	e = client.Call("JRPC.GET_PREDECESSOR", Self, &x)
	if e != nil {
		log.Fatal("dialing", e)
	}

	if x.NodeID > Self.NodeID && x.NodeID < Successor.NodeID {
		Successor = x
	}

	client.Close()

	//notify the successor, n is its predecessor
	client, e = jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))

	if e != nil {
		log.Fatal("dialing", e)
	}
	e = client.Call("JRPC.NOTIFY_PREDECESSOR", Self, &Predecessor)
	if e != nil {
		log.Fatal("dialing", e)
	}
	client.Close()

	//data transfer from its successor
	client, e = jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))

	if e!= nil{
		log.Fatal("dialing",e)
	}
	e = client.Call("JRPC.DATA_TRANSFER_FROM_SUCCESSOR", Self, &dict3)
	client.Close()

	//calculate keys of data
	KR_Hash_All()
	//write data into a database
	rewrite()

	//notify the predecessor, n is its successor
	client, e = jsonrpc.Dial(NodeParams.Protocol, Predecessor.IpAddress+":"+strconv.Itoa(Predecessor.Port))

	if e != nil {
		log.Fatal("dialing", e)
	}

	e = client.Call("JRPC.NOTIFY_SUCCESSOR", Self, &x)
	client.Close()
}

func (r *JRPC) NOTIFY_PREDECESSOR(request *ChordNode, response *ChordNode) error {

	if Predecessor == Self {

		response.IpAddress = Predecessor.IpAddress
		response.Port = Predecessor.Port
		response.NodeID = Predecessor.NodeID
		Predecessor.IpAddress = request.IpAddress
		Predecessor.Port = request.Port
		Predecessor.NodeID = request.NodeID
	} else {

		/*if Predecessor.NodeID > Self.NodeID {

			if (request.NodeID > Predecessor.NodeID && request.NodeID > Self.NodeID) || (request.NodeID < Predecessor.NodeID && request.NodeID < Self.NodeID) {
				response.IpAddress = Predecessor.IpAddress
				response.Port = Predecessor.Port
				response.NodeID = Predecessor.NodeID
				Predecessor.IpAddress = request.IpAddress
				Predecessor.Port = request.Port
				Predecessor.NodeID = request.NodeID

			}

		} else {

			if request.NodeID > Predecessor.NodeID && request.NodeID < Self.NodeID {*/

				response.IpAddress = Predecessor.IpAddress
				response.Port = Predecessor.Port
				response.NodeID = Predecessor.NodeID
				Predecessor.IpAddress = request.IpAddress
				Predecessor.Port = request.Port
				Predecessor.NodeID = request.NodeID}
			/*}

		}

	}*/
	fmt.Printf("Predecessor: %s:%d | NodeID: %d \n", Predecessor.IpAddress, Predecessor.Port, Predecessor.NodeID)
	fmt.Printf("Successor:    %s:%d | NodeID: %d \n", Successor.IpAddress, Successor.Port, Successor.NodeID)
	return nil

}

func (r *JRPC) NOTIFY_SUCCESSOR(request *ChordNode, response *ChordNode) error {

	if Successor == Self {
		Successor.IpAddress = request.IpAddress
		Successor.Port = request.Port
		Successor.NodeID = request.NodeID
	} else {

		/*if Self.NodeID > Successor.NodeID {

			if (request.NodeID > Self.NodeID && request.NodeID > Successor.NodeID) || (request.NodeID < Self.NodeID && request.NodeID < Successor.NodeID) {

				Successor.IpAddress = request.IpAddress
				Successor.Port = request.Port
				Successor.NodeID = request.NodeID

			}

		} else {

			if (request.NodeID > Self.NodeID && request.NodeID < Successor.NodeID {*/

				Successor.IpAddress = request.IpAddress
				Successor.Port = request.Port
				Successor.NodeID = request.NodeID
}
			/*}

		}

	}*/
	fmt.Printf("Predecessor: %s:%d | NodeID: %d \n", Predecessor.IpAddress, Predecessor.Port, Predecessor.NodeID)
	fmt.Printf("Successor:    %s:%d | NodeID: %d \n", Successor.IpAddress, Successor.Port, Successor.NodeID)
	return nil

}

//data transfer from successor
func (r *JRPC) DATA_TRANSFER_FROM_SUCCESSOR(request *ChordNode, response *Dict3) error {

	var index []int
	index = index[:0]

	for i := 0; i < len(dict3); i++ {
		if (krhash[i] <= Self.NodeID && krhash[i] > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && krhash[i] > Predecessor.NodeID && krhash[i] < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && krhash[i] < Self.NodeID && (krhash[i] + keybits) > Predecessor.NodeID) {
		
		} else {
			//fmt.Printf("%d",i)
			*response = append(*response, dict3[i])
			index = append(index, i)
		}
	}
	for i := 0; i < len(index); i++ {

		dict3 = append(dict3[:index[i]], dict3[index[i]+1:]...)
		for j := i + 1; j < len(index); j++ {
			index[j] = index[j] - 1
		}
	}

	KR_Hash_All()
	rewrite()

	return nil

}

//data transfer from predecessor
func (r *JRPC) DATA_TRANSFER_FROM_PREDECESSOR(request *Dict3, response *ChordNode) error {

	var index []int
	index = index[:0]

	for i := 0; i < len(*request); i++ {
		dict3 = append(dict3, (*request)[i])
	}

	KR_Hash_All()
	rewrite()

	return nil

}

//data transfer from predecessor
func (r *JRPC) DATA_TRANSFER_FROM_PREDECESSOR_REVERSE(request *ChordNode, response *Dict3) error {

	var index []int
	index = index[:0]

	for i := 0; i < len(dict3); i++ {

			//fmt.Printf("%d",i)
		*response = append(*response, dict3[i])
		index = append(index, i)
		
	}
	for i := 0; i < len(index); i++ {

		dict3 = append(dict3[:index[i]], dict3[index[i]+1:]...)
		for j := i + 1; j < len(index); j++ {
			index[j] = index[j] - 1
		}
	}

	KR_Hash_All()
	rewrite()

	return nil

}

//Fix finger tables by nodes and their successors
func (r *JRPC) FIX_FINGER(g *ChordArray, o *ChordNode) error {

		//fmt.Printf("%d\n",g.Successor.NodeID)
		var n ChordNode
		var s ChordNode	
		var t ChordNode
		var count int 
		count = 0

		for i := 0; i < BITSIZE; i++ { //because now I set bits as 7 (7-bit ring). There are (at most) 7 rows/entries in the finger table

			n = Self
			s = Successor

			f := float64(i)
			t.NodeID = (n.NodeID + int(math.Pow(2., f))) % keybits //calculate id for each row
			//fmt.Printf("%d: %d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID)
			//fmt.Printf("%d\n",t.NodeID)

			if i == 0 {
				Finger[i] = s // the first row of finger table is its successor
				continue
			} else {

				for {
					//this part is used to find the successor
					if (t.NodeID > n.NodeID && t.NodeID <= s.NodeID) && (n.NodeID <= s.NodeID) {
						Finger[i] = s
						break
					} else if t.NodeID == n.NodeID {
						Finger[i] = n
						break
					} else if t.NodeID == s.NodeID {
						Finger[i] = s
						break
					} else if n.NodeID > s.NodeID {
						if t.NodeID > n.NodeID && t.NodeID < (s.NodeID+keybits) {
							Finger[i] = s
							break
						} else if t.NodeID < s.NodeID && (t.NodeID+keybits) > n.NodeID {
							Finger[i] = s
							break
						}
					}
					n = s
//fmt.Printf("%d: %d,%d,%d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID,Self.NodeID,g.Self.NodeID)
					if (s.NodeID != Self.NodeID) && (s.NodeID!=g.Self.NodeID){
						client, e := jsonrpc.Dial(NodeParams.Protocol, s.IpAddress+":"+strconv.Itoa(s.Port))
						if e != nil {
							log.Fatal("dialing", e)
						}
						e = client.Call("JRPC.GET_SUCCESSOR", t, &s)
						client.Close()
					} else if (s.NodeID != Self.NodeID) && (s.NodeID==g.Self.NodeID) {
						s.NodeID = g.Successor.NodeID
						s.IpAddress = g.Successor.IpAddress
						s.Port =g.Successor.Port
//fmt.Printf("%d: %d,%d,%d,%d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID,Self.NodeID,g[0].NodeID,g[1].NodeID)
					} else {
						s = Successor
					}
						//fmt.Printf("%d,%d\n",n.NodeID, s.NodeID+keybits)
//fmt.Printf("%d: %d,%d,%d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID,Self.NodeID,g[0].NodeID)
				}
			}
			count = count + 1
		}
	//PRINT_FINGERTABLE()
	return nil
}

//Look up function: allow complete/uncomplete keys. If uncomplete keys are input, the blank parts are filled in automatically and all values in which uncomplete keys are contained will be returned to client
func (r *JRPC) LOOKUP(d *Operation, o *Get) error {

	var flag bool
	flag = false
	var index int

	var hresult ChordNode	
	var hashnum []int
	hashnum = hashnum[:0]

	var tmpo []DICT3Item
	tmpo = tmpo[:0]

	var loop int
	
	o.Result = nil
	//o.Id = d.Id
	o.Error = nil

	if d.Params[0].(string) != "" && d.Params[1].(string) != "" {
		loop = 1
		//fmt.Printf("%d\n", loop)
		//hashnum = int[0:loop]
		hashnum=append(hashnum,int(float64(KRHash_Key(d.Params[0].(string))) * math.Pow(2.,float64(BITSIZE/2)) + float64(KRHash_Rel(d.Params[1].(string)))))
		//fmt.Printf("%d, %d", loop, hashnum[0])
	} else if d.Params[0].(string) == "" {
		loop= int(math.Pow(2.,BITSIZE/2))
		//hashnum = hashnum[:loop]
		for j:=0;j<loop;j++ {
			hashnum=append(hashnum,int(float64(j)*math.Pow(2.,float64(BITSIZE/2)) +float64( KRHash_Rel(d.Params[1].(string)))))
			//fmt.Printf("%f, %f, %d\n",float64(j), math.Pow(2.,float64(BITSIZE/2)),hashnum[j])
		}
	} else if d.Params[1].(string) == "" {
		loop= int( math.Pow(2.,BITSIZE/2))
		//hashnum = hashnum[:loop]
		for j:=0;j<loop;j++ {
			hashnum=append(hashnum,int(float64(KRHash_Key(d.Params[0].(string))) * math.Pow(2.,float64(BITSIZE/2)) + float64(j)))
		}
	}
	
	for k:=0;k<loop;k++ {
		
		//fmt.Printf("%d, %d", loop, hashnum[k])
		hresult.NodeID = hashnum[k]
		//fmt.Printf("%d\n",hashnum[k])
	

		if (hresult.NodeID <= Self.NodeID && hresult.NodeID > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID > Predecessor.NodeID && hresult.NodeID < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID < Self.NodeID && (hresult.NodeID + keybits) > Predecessor.NodeID) || (Self.NodeID == Predecessor.NodeID) || (hresult.NodeID==Self.NodeID){

			flag = false

			for i := 0;i<len(dict3);i++ {
				//fmt.Printf("%d, %d\n",krhash[i] ,hashnum[k])
	
				if krhash[i] == hresult.NodeID{//d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) {//krhash[i] == hashnum[k]{//
					index = i
					flag = true
					break
				}
			}

			if flag != false {
				//fmt.Printf("search: %d, %d\n",krhash[index] ,hashnum[k])
				o.Result = append(o.Result,dict3[index])
				//o.Id = d.Id
				o.Error = nil

			} /*else {
				o.Result = nil
				o.Id = d.Id
				o.Error = nil
			}*/
	
		} else {
	
			var successor_keyrel ChordNode

			client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.FIND_SUCCESSOR", hresult, &successor_keyrel)
			client.Close()
			//fmt.Printf("%d: %s:%d\n",hresult.NodeID,Successor.IpAddress,Successor.Port)
			//fmt.Printf("%d: \n",hresult.NodeID)
			//fmt.Printf("%d: %s:%d\n",hresult.NodeID, successor_keyrel.IpAddress,successor_keyrel.Port)
			client, e = jsonrpc.Dial(NodeParams.Protocol, successor_keyrel.IpAddress+":"+strconv.Itoa(successor_keyrel.Port))
			if e != nil {
				//client.Close()
				log.Fatal("dialing", e)
			} 

			var tmpnode ChordNode
			tmpnode.NodeID = hashnum[k]


			e = client.Call("JRPC.LOOKUP_DATA",tmpnode , &tmpo)
			fmt.Printf("%d: %s:%d\n",hresult.NodeID, successor_keyrel.IpAddress,successor_keyrel.Port)
			
			//if tmpo!=nil{
				for i:=0;i<len(tmpo);i++ {
					if tmpo[i]!=nil{
						o.Result = append(o.Result, tmpo[i])
					}
				}
			//}

			client.Close()
			
		}
	}


		tmpo=tmpo[:0]
		return nil
}

//Look up data
func (r *JRPC) LOOKUP_DATA(d *ChordNode, o *DICT3Item) error {

	var flag bool
	flag = false
	var index int
	//fmt.Printf("%s\n",o.Result)
	var hresult ChordNode	
	hresult.NodeID = d.NodeID
	
	//o.Result = nil
	//o.Id = 0
	//o.Error = nil

	if (hresult.NodeID <= Self.NodeID && hresult.NodeID > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID > Predecessor.NodeID && hresult.NodeID < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID < Self.NodeID && (hresult.NodeID + keybits) > Predecessor.NodeID) || (Self.NodeID == Predecessor.NodeID) {

		flag = false
		for i := 0;i<len(dict3);i++ {

			if d.NodeID == krhash[i]{//d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string){
				index = i
				flag = true
				break
			}
		}
		if flag != false {
			//*o = append(*o,dict3[index])
			*o = append(*o,dict3[index])
			//fmt.Printf("%s\n",o)
			//o.Id = d.Id
			//o.Error = nil
		}  else {
			*o = append(*o,nil)
			//*o = *o
			//*o = append(*o,nil)
		}
	} else {
	
		var successor_keyrel ChordNode

		client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.FIND_SUCCESSOR", hresult, &successor_keyrel)
		client.Close()
		
		client, e = jsonrpc.Dial(NodeParams.Protocol, successor_keyrel.IpAddress+":"+strconv.Itoa(successor_keyrel.Port))

		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.LOOKUP_DATA", d, &o)
		client.Close()
		

		
	}
		return nil

}

//Insert a data
func (r *JRPC) INSERT(d *Operation, o *Get) error {

	var hresult ChordNode

	hresult.NodeID = int(float64(KRHash_Key(d.Params[0].(string))) * math.Pow(2.,float64(BITSIZE/2)) + float64(KRHash_Rel(d.Params[1].(string))))	

	if (hresult.NodeID <= Self.NodeID && hresult.NodeID > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID > Predecessor.NodeID && hresult.NodeID < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID < Self.NodeID && (hresult.NodeID + keybits) > Predecessor.NodeID) || (Self.NodeID == Predecessor.NodeID) {

		var flag bool
		flag = false
		//var index int

		for i := 0; i < len(dict3); i++ {

			if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { 
				flag = true
				break
			}
		}

		if flag != false {
			x := []interface{}{false}
			o.Result = x
			//o.Id = d.Id
			o.Error = nil

		} else {
			x := []interface{}{true}
			o.Result = x
			//o.Id = d.Id
			o.Error = nil
			dict3 = append(dict3, d.Params)
		}

		KR_Hash_All()
		rewrite()	

	} else{
		var successor_keyrel ChordNode

		client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.FIND_SUCCESSOR", hresult, &successor_keyrel)
		client.Close()

		client, e = jsonrpc.Dial(NodeParams.Protocol, successor_keyrel.IpAddress+":"+strconv.Itoa(successor_keyrel.Port))
		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.INSERT_DATA", d, &o)
		client.Close()
		
	}

	return nil
}


func (r *JRPC) INSERT_DATA(d *Operation, o *Get) error {
	var flag bool
	flag = false
	//var index int

	for i := 0; i < len(dict3); i++ {

		if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { 
			flag = true
			break
		}
	}

	if flag != false {
		x := []interface{}{false}
		o.Result = x
		//o.Id = d.Id
		o.Error = nil

	} else {
		x := []interface{}{true}
		o.Result = x
		//o.Id = d.Id
		o.Error = nil
		dict3 = append(dict3, d.Params)
	}

	KR_Hash_All()
	rewrite()

	return nil
}

func (r *JRPC) INSERTORUPDATE(d *Operation, g *Get) error {

	var hresult ChordNode

	hresult.NodeID = int(float64(KRHash_Key(d.Params[0].(string))) * math.Pow(2.,float64(BITSIZE/2)) + float64(KRHash_Rel(d.Params[1].(string))))	

	if (hresult.NodeID <= Self.NodeID && hresult.NodeID > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID > Predecessor.NodeID && hresult.NodeID < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID < Self.NodeID && (hresult.NodeID + keybits) > Predecessor.NodeID) || (Self.NodeID == Predecessor.NodeID) {

		var flag bool
		flag = false
		var index int

		for i := 0; i < len(dict3); i++ {

			//fmt.Printf("%s\n",d.Params[0])
			//fmt.Printf("%s\n",dict3[i][0])
			if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { //== true && strings.EqualFold(d.p[1],dict3[i].p[1]) == true {
				//d.Params[2] = dict3[i][2]
			//tmpVal := dict3[i][2].(map[string]interface{})
			//if tmpVal["permission"].(string) == "RW" {
				index = i
				flag = true}
				break
			//}

		}

		//var x []interface{}

		if flag != false {
			tmpVal := dict3[index][2].(map[string]interface{})
			if tmpVal["permission"].(string) == "RW" {
				dict3[index] = d.Params
			}

		} else {

			dict3 = append(dict3, d.Params)
		}
		//fmt.Printf("Results: %v\n", dict3)

		KR_Hash_All()
		rewrite()	

	} else{
		var successor_keyrel ChordNode

		client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.FIND_SUCCESSOR", hresult, &successor_keyrel)
		client.Close()

		client, e = jsonrpc.Dial(NodeParams.Protocol, successor_keyrel.IpAddress+":"+strconv.Itoa(successor_keyrel.Port))
		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.INSERTORUPDATE_DATA", d, &g)
		client.Close()
		
	}

	return nil
}

func (r *JRPC) INSERTORUPDATE_DATA(d *Operation, g *Get) error {
	var flag bool
	flag = false
	var index int

	for i := 0; i < len(dict3); i++ {

		//fmt.Printf("%s\n",d.Params[0])
		//fmt.Printf("%s\n",dict3[i][0])
		if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { //== true && strings.EqualFold(d.p[1],dict3[i].p[1]) == true {
			//d.Params[2] = dict3[i][2]
			//tmpVal := dict3[i][2].(map[string]interface{})
			//if tmpVal["permission"].(string) == "RW" {
			index = i
			flag = true//}
			break
		}

	}

	//var x []interface{}
	//tmpVal = dict3[index][2].(map[string]interface{})
	//if tmpVal["permission"].(string) == "RW" {
		if flag != false {
			dict3[index] = d.Params

		} else {

			dict3 = append(dict3, d.Params)
		}
	//}
	//fmt.Printf("Results: %v\n", dict3)
	KR_Hash_All()
	rewrite()

	return nil
}
func (r *JRPC) DELETE(d *Operation, g *Get) error {

	var hresult ChordNode

	hresult.NodeID = int(float64(KRHash_Key(d.Params[0].(string))) * math.Pow(2.,float64(BITSIZE/2)) + float64(KRHash_Rel(d.Params[1].(string))))	

	if (hresult.NodeID <= Self.NodeID && hresult.NodeID > Predecessor.NodeID) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID > Predecessor.NodeID && hresult.NodeID < (Self.NodeID+keybits)) || (Self.NodeID<Predecessor.NodeID && hresult.NodeID < Self.NodeID && (hresult.NodeID + keybits) > Predecessor.NodeID) || (Self.NodeID == Predecessor.NodeID) {

	var flag bool
	flag = false
	var index int

	for i := 0; i < len(dict3); i++ {

		//fmt.Printf("%s\n",d.Params[0])
		//fmt.Printf("%s\n",dict3[i][0])
		if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { //== true && strings.EqualFold(d.p[1],dict3[i].p[1]) == true {
			//d.Params[2] = dict3[i][2]
			index = i
			flag = true
			break
		}

	}

	//var x []interface{}

	if flag != false {
		dict3 = append(dict3[:index], dict3[index+1:]...)

	}
	//fmt.Printf("Results: %v\n", dict3)
	KR_Hash_All()
	rewrite()	

	} else{
		var successor_keyrel ChordNode

		client, e := jsonrpc.Dial(NodeParams.Protocol, Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.FIND_SUCCESSOR", hresult, &successor_keyrel)
		client.Close()

		client, e = jsonrpc.Dial(NodeParams.Protocol, successor_keyrel.IpAddress+":"+strconv.Itoa(successor_keyrel.Port))
		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.DELETE_DATA", d, &g)
		client.Close()
		
	}

	return nil
}

func (r *JRPC) DELETE_DATA(d *Operation, g *Get) error {
	var flag bool
	flag = false
	var index int

	for i := 0; i < len(dict3); i++ {

		//fmt.Printf("%s\n",d.Params[0])
		//fmt.Printf("%s\n",dict3[i][0])
		if d.Params[0].(string) == dict3[i][0].(string) && d.Params[1].(string) == dict3[i][1].(string) { //== true && strings.EqualFold(d.p[1],dict3[i].p[1]) == true {
			//d.Params[2] = dict3[i][2]
			index = i
			flag = true
			break
		}

	}

	//var x []interface{}

	if flag != false {
		dict3 = append(dict3[:index], dict3[index+1:]...)

	}
	//fmt.Printf("Results: %v\n", dict3)

	KR_Hash_All()
	rewrite()

	return nil
}


func (r *JRPC) PURGE(d *Operation, g *Get) error {
	//make another Dict3 type object that stores the records from 
	//the dictionary that have been accessed within 6 hours.
	var copy Dict3

	for i := 0; i < len(dict3); i++ {
		tmpVal := dict3[i][2].(map[string]interface{})
		//fmt.Println(tmpVal["accessed"])

		//parse the access time string in the value
		form := "1/02/2006, 15:04:05"
		t, e := time.Parse(form, tmpVal["accessed"].(string))
		//fmt.Printf("%d-%02d-%02dT%02d:%02d:%02d-00:00\n",
        //t.Year(), t.Month(), t.Day(),
        //t.Hour(), t.Minute(), t.Second())
        fmt.Println(e)

        //Find the time duration since the access time until now
        duration := time.Since(t)
		fmt.Println(duration.Hours())
		//Only keep the files that have been accessed within user specified time in hours
		durationthreshold, _ := strconv.Atoi(d.Params[0].(string))

        if duration.Hours() < float64(durationthreshold ) {
        	fmt.Println(duration.Hours())
        	copy = append(copy, dict3[i])
        }
	}
	dict3 = copy
    //fmt.Println(dict3)

	KR_Hash_All()
	rewrite()

	return nil
}

func (r *JRPC) LISTKEYS(d *Operation, g *Get) error {

	g.Result = nil
	for i := 0; i < len(dict3); i++ {

		g.Result = append(g.Result, dict3[i][0])

	}
	//fmt.Printf("Results: %v\n", g.Result)
	//g.Id = d.Id
	g.Error = nil

	var S ChordNode //successor
	S = Successor
	var N ChordNode
	N = Self

	var tmpg *Get

	for {
		if S.NodeID == Self.NodeID {break}

		client, e := jsonrpc.Dial(NodeParams.Protocol, S.IpAddress+":"+strconv.Itoa(S.Port))
		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.LISTKEYS_DATA", d, &tmpg)
		client.Close()

		for i := 0; i < len(tmpg.Result); i++ {

			g.Result = append(g.Result, tmpg.Result[i])

		}
		client, e = jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.GET_SUCCESSOR", N, &S)
		client.Close()

		if S.NodeID == Self.NodeID {break}

	}

	return nil
}

func (r *JRPC) LISTKEYS_DATA(d *Operation, g *Get) error {

	g.Result = nil
	for i := 0; i < len(dict3); i++ {

		g.Result = append(g.Result, dict3[i][0])

	}
	//fmt.Printf("Results: %v\n", g.Result)
	//g.Id = d.Id
	g.Error = nil

	return nil
}

func (r *JRPC) LISTIDS(d *Operation, g *Get) error {

	var p []string

	//g.Result = nil
	//p = nil

	var ttt []interface{}
	//ttt = nil

	for i := 0; i < len(dict3); i++ {
		p = append(p, dict3[i][0].(string))
		p = append(p, dict3[i][1].(string))

		//g.Result = append(g.Result, p)
		ttt = append(ttt,p)

		//fmt.Printf("Results: %v\n", g.Result[i])

		//p = p[:0]
	}
	g.Result = append(g.Result, p)
	fmt.Printf("Results: %s\n", ttt)
	fmt.Printf("Results: %s\n", g.Result)
	//g.Id = d.Id
	g.Error = nil

	var S ChordNode //successor
	S = Successor
	var N ChordNode
	N = Self
	var tmpg *Get

	for {
		if S.NodeID == Self.NodeID {break}

		client, e := jsonrpc.Dial(NodeParams.Protocol, S.IpAddress+":"+strconv.Itoa(S.Port))
		if e != nil {
			//client.Close()
			log.Fatal("dialing", e)
		} 
		e = client.Call("JRPC.LISTIDS_DATA", d, &tmpg)
		client.Close()
		for i := 0; i < len(tmpg.Result); i++ {

			g.Result = append(g.Result, tmpg.Result[i])

		}
		client, e = jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.GET_SUCCESSOR", N, &S)
		client.Close()

		if S.NodeID == Self.NodeID {break}

	}
	return nil
}

func (r *JRPC) LISTIDS_DATA(d *Operation, g *Get) error {

	var p []string

	g.Result = nil
	for i := 0; i < len(dict3); i++ {

		p = append(p, dict3[i][0].(string))
		p = append(p, dict3[i][1].(string))

		g.Result = append(g.Result, p)

		p = p[:0]

	}

	//g.Id = d.Id
	g.Error = nil

	return nil
}

func FIX_LOCAL_FINGER(N ChordNode, S ChordNode) {

	var Init_Self ChordNode
	Init_Self = N

	var Init_Successor ChordNode
	Init_Successor = S

	var n ChordNode
	var s ChordNode
	var t ChordNode

	for i := 0; i < BITSIZE; i++ { //because now I set bits as 7 (7-bit ring). There are (at most) 7 rows/entries in the finger table
		n = N
		s = S

		f := float64(i)
		t.NodeID = (n.NodeID + int(math.Pow(2., f))) % keybits //calculate id for each row
		//fmt.Printf("%d: %d,%d,%d\n",i, t.NodeID,n.NodeID, s.NodeID)
		//fmt.Printf("%d\n",t.NodeID)

		if i == 0 {
			Finger[i] = s // the first row of finger table is its successor
			continue
		} else {

		for {
					//this part is used to find the successor
			if (t.NodeID > n.NodeID && t.NodeID <= s.NodeID) && (n.NodeID <= s.NodeID) {
					Finger[i] = s
					break
			} else if t.NodeID == n.NodeID {
					Finger[i] = n
					break
			} else if t.NodeID == s.NodeID {
					Finger[i] = s
					break
			} else if n.NodeID > s.NodeID {
					if t.NodeID > n.NodeID && t.NodeID < (s.NodeID+keybits) {
						Finger[i] = s
						break
					} else if t.NodeID < s.NodeID && (t.NodeID+keybits) > n.NodeID {
						Finger[i] = s
						break
				}
			}
			n = s

			if s.NodeID != Init_Self.NodeID {
				client, e := jsonrpc.Dial(NodeParams.Protocol, s.IpAddress+":"+strconv.Itoa(s.Port))
				//fmt.Printf("%s\n",s.IpAddress+":"+strconv.Itoa(s.Port))
				if e != nil {
					log.Fatal("dialing", e)
				}
				e = client.Call("JRPC.GET_SUCCESSOR", t, &s)
				client.Close()
			} else {
				s = Init_Successor
				}
					//fmt.Printf("%d,%d\n",n.NodeID, s.NodeID+keybits)
			}
		}
		//count = count + 1
		}

		// fix the finger table
		for {
			//fmt.Printf("%d\n",count)
			N = S

			if N.NodeID == Init_Self.NodeID { //which means we already fixed all the nodes
				break
			}

			client, e := jsonrpc.Dial(NodeParams.Protocol, N.IpAddress+":"+strconv.Itoa(N.Port))

			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.GET_SUCCESSOR", Init_Self, &S)
			client.Close()

			client, e = jsonrpc.Dial(NodeParams.Protocol, N.IpAddress+":"+strconv.Itoa(N.Port))

			if e != nil {
				log.Fatal("dialing", e)
			}
			var chordarray ChordArray
			chordarray.Self = Init_Self
			chordarray.Successor = Init_Successor
			e = client.Call("JRPC.FIX_FINGER", chordarray, &t)
			client.Close()
		}

}

//shut down one node based on the input node id of client, and data stored in that node will be transfered to its successor. 
func (r *JRPC) SHUTDOWN(d *Operation, g *Get) error {

	var tmp ChordNode

	var d_tmp ChordNode
	id,_   := strconv.Atoi(d.Params[0].(string))
	d_tmp.NodeID = id

	fmt.Printf("%d\n",id)

	if d_tmp.NodeID == Self.NodeID {
			
		if Successor.NodeID == Self.NodeID {

			os.Exit(1)
			return nil
		}

		client, e := jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.DATA_TRANSFER_FROM_PREDECESSOR", dict3, &tmp)
		client.Close()	

		dict3 = dict3[:0]
		KR_Hash_All()
		rewrite()

		client, e = jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.NOTIFY_PREDECESSOR", Predecessor, &tmp)
		client.Close()		

		client, e = jsonrpc.Dial(NodeParams.Protocol,Predecessor.IpAddress+":"+strconv.Itoa(Predecessor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.NOTIFY_SUCCESSOR", Successor, &tmp)
		client.Close()	

		FIX_LOCAL_FINGER(Self, Successor)

		os.Exit(1)

		return nil	
	} else {

		var S ChordNode		

		client, e := jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.FIND_SUCCESSOR", d_tmp, &S)
		client.Close()	

		//fmt.Printf("%s:%d\n", S.IpAddress, S.Port)

		if S.NodeID == d_tmp.NodeID {

			fmt.Printf("node id: %d \n",S.NodeID)
			
			var S_successor ChordNode
			var P_predecessor ChordNode

			client, e = jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.GET_SUCCESSOR", Self, &S_successor)
			client.Close()

			fmt.Printf("s node id: %d \n",S_successor.NodeID)

			client, e = jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.GET_PREDECESSOR", Self, &P_predecessor)
			client.Close()


			fmt.Printf("p node id: %d \n",P_predecessor.NodeID)

			tmp = Self

			if S_successor.NodeID == Self.NodeID {

				client, e := jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
				if e != nil {
					log.Fatal("dialing", e)
				}
				var tmp_dict3 Dict3
				e = client.Call("JRPC.DATA_TRANSFER_FROM_PREDECESSOR_REVERSE", tmp, &tmp_dict3)
				for kk :=0;kk<len(tmp_dict3);kk++ {
				dict3 = append(dict3, tmp_dict3[kk])}
				client.Close()	

				fmt.Printf("equal node id: %d \n",S.NodeID)

				Predecessor = P_predecessor

				KR_Hash_All()
				rewrite()

				//tmp = Self
				tmp.Port = 0

			}
			if P_predecessor.NodeID == Self.NodeID {

				Successor = S_successor
				//tmp = Self
				tmp.Port = -1

			}
			if S_successor.NodeID == Self.NodeID && P_predecessor.NodeID == Self.NodeID {
				
				tmp.Port = -2

			}
fmt.Printf("equal node id: %d \n",S.NodeID)
			client, e = jsonrpc.Dial(NodeParams.Protocol,S.IpAddress+":"+strconv.Itoa(S.Port))
			if e != nil {
				log.Fatal("dialing", e)
			}
			e = client.Call("JRPC.SHUTDOWN_DATA", tmp, &S)
			client.Close()	

			if Self.NodeID == Successor.NodeID {

				for i := 0; i < BITSIZE; i++ {
					Finger[i] = Self 
				}	

			} else {
				FIX_LOCAL_FINGER(Self, Successor)			
			}
PRINT_FINGERTABLE()
			return nil
		} else {
		
			return nil
		}

	}
}

func (r *JRPC) SHUTDOWN_DATA(d *ChordNode, g *ChordNode) error {

	var tmp ChordNode

	fmt.Printf("n node is %d \n", d.NodeID)

	if d.Port !=0  && d.Port != -2{

		client, e := jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.DATA_TRANSFER_FROM_PREDECESSOR", dict3, &tmp)
		client.Close()	

	}

	dict3 = dict3[:0]
	KR_Hash_All()
	rewrite()

	if d.Port !=0  && d.Port != -2{
		client, e := jsonrpc.Dial(NodeParams.Protocol,Successor.IpAddress+":"+strconv.Itoa(Successor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		e = client.Call("JRPC.NOTIFY_PREDECESSOR", Predecessor, &tmp)
		client.Close()		
	}

	if d.Port != -1 && d.Port != -2{
		fmt.Printf("p node is %d \n", Predecessor.NodeID)		
		client, e := jsonrpc.Dial(NodeParams.Protocol,Predecessor.IpAddress+":"+strconv.Itoa(Predecessor.Port))
		if e != nil {
			log.Fatal("dialing", e)
		}
		fmt.Printf("s node is %d \n", Successor.NodeID)
		e = client.Call("JRPC.NOTIFY_SUCCESSOR", Successor, &tmp)
		client.Close()	
	}
	os.Exit(1)
	return nil
}

//update the database
func rewrite() {
	file, _ := json.Marshal(dict3)
	ioutil.WriteFile(NodeParams.PersistentStorageContainer.File, file, 0664)
}

func main() {
	config_file := os.Args[1]
	StartingIpAddress = os.Args[2]
	StartingPort, _ = strconv.Atoi(os.Args[3])

	// Keybits is the equivalent of 2^M, in s = successor(NodeID + 2^(i−1) ) mod 2^M, where M is the N-bit size of the ring; For example, this is a 7-bit chord ring; because bit size is 7 in this implementation
	keybits = int(math.Pow(2., BITSIZE)) // BITSIZE = 8 , which is the size of our chord ring, i.e. we are using a 7-bit chord ring
	fmt.Printf("Keybits: %d\n", keybits)

	file, e := ioutil.ReadFile(config_file)
	if e != nil {
		fmt.Println("Error: Cannot Find Configuration File")
		os.Exit(1)
	}
	fmt.Println("Opened Configuration File successfully")
	json.Unmarshal(file, &NodeParams)
	//fmt.Printf("Results: %v\n", NodeParams)

	file2, e := ioutil.ReadFile(NodeParams.PersistentStorageContainer.File)
	if e != nil {
		fmt.Println("Error: Cannot Find DICT3 File")
		os.Exit(1)
	}
	fmt.Println("Opened DICT3 File successfully")
	json.Unmarshal(file2, &dict3)
	//fmt.Printf("Results: %v\n", dict3[0][0])

	//jsonrpc service object; JRPC is an int ; jrpc is actually the Dict3 Service that provides methods (or remote procedures) such as INSERT, LOOKUP etc
	jrpc := new(JRPC)
	rpc.Register(jrpc)

	tcpAddr, e := net.ResolveTCPAddr(NodeParams.Protocol, NodeParams.IpAddress+":"+strconv.Itoa(NodeParams.Port))
	JOIN()

	listener, e := net.ListenTCP(NodeParams.Protocol, tcpAddr)
	if e != nil {
		fmt.Printf("Error: TCP Connection")
	}

	for {

		conn, e := listener.Accept()
		if e != nil {
			continue
		}
		jsonrpc.ServeConn(conn)
	}

}
