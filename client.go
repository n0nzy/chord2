package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"strings"
)

//chord node type
type ChordNode struct {
	NodeID    int
	IpAddress string
	Port      int
}

type ConfigParamsType struct {
	ServerID                   string
	Protocol                   string
	IpAddress                  string
	Port                       int
	PersistentStorageContainer PersistentStorageContainerType
	Methods                    []string
}

type PersistentStorageContainerType struct {
	File string
}

type Dict3 []DICT3Item

type DICT3Item []interface{}

type Operation struct {
	Method string
	Params DICT3Item
	//Id     int
}
type Get struct {
	Result DICT3Item
	//Id     int
	Error  error
}

var NodeParamsType ConfigParamsType
var dict3 DICT3Item //DICT3Item
var re Get
var op Operation

func main() {
	file, e := ioutil.ReadFile("./config.5550.json")
	if e != nil {
		fmt.Print("Error: Cannot Find Configuration File\n")
		os.Exit(1)
	}
	fmt.Printf("Opened Configuration File successfully\n")
	json.Unmarshal(file, &NodeParamsType)
	//fmt.Printf("Results: %v\n", NodeParamsType)



	reader := bufio.NewReader(os.Stdin)

	for {


		re.Result = nil
		//re.Id = -1
		re.Error = nil

		fmt.Print("Please Enter a properly-structured JSON-RPC message: ")
		text, e := reader.ReadString('\n')
		if e != nil {
			fmt.Print("Error: Input\n")
			os.Exit(1)
		}
		json.Unmarshal([]byte(text), &op)

		//fmt.Printf("Results: %v\n", op.Method)
		s := strings.ToUpper(op.Method)

		var flag bool
		flag = true

		for i := 0; i < len(NodeParamsType.Methods); i++ {
			if strings.EqualFold(strings.ToUpper(NodeParamsType.Methods[i]), s) == true {
				flag = false
			}
		}

		if flag == true {
			fmt.Printf("Please input a valid method: ")
			continue
		}

		re2 := new(Get)

		client, err := jsonrpc.Dial(NodeParamsType.Protocol, NodeParamsType.IpAddress+":"+strconv.Itoa(NodeParamsType.Port))
		if err != nil {
			log.Fatal("dialing", err)
		}

		serverCall := client.Go("JRPC."+s, op, re2, nil)

		serverCall = <-serverCall.Done
		if serverCall.Error != nil {
			log.Fatal("Server call returned an error: ", serverCall.Error.Error())
		}
		client.Close()

		if strings.EqualFold(strings.ToUpper(NodeParamsType.Methods[2]), s) != true && strings.EqualFold(strings.ToUpper(NodeParamsType.Methods[3]), s) != true && strings.EqualFold(strings.ToUpper(NodeParamsType.Methods[6]), s) != true {
			file, _ = json.Marshal(re2)
			fmt.Printf("Results: %v\n", string(file))
			//fmt.Printf("Results: %v\n", response)

			//divCall := client.Go("Arith.Divide", args, quotient, nil)
			//replyCall := <-divCall.Done	// will be equal to divCall
		}

		continue
	}

}
