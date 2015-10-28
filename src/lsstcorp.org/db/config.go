package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type jsonobject struct {
	Database DatabaseDesc
}

type DatabaseDesc struct {
	Host       string
	User       string
	Pass       string
	Port       int
	SizeGB     float32
	Name       string
	TablesDesc []TablesDesc
}

type TablesDesc struct {
	ColumnsDesc []ColumnsDesc
}

type ColumnsDesc struct {
	Mode    string
	Width   int
	SqlType string
}

const (
	TABLE_PREFIX string = "Table_"
)

// Main function
// I realize this function is much too simple I am simply at a loss to

func ReadConfig(configFile string) jsonobject {
	file, e := ioutil.ReadFile(configFile)
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}
	fmt.Printf("%s\n", string(file))

	//m := new(Dispatch)
	//var m interface{}
	var jsontype jsonobject
	err := json.Unmarshal(file, &jsontype)
	if err != nil {
		log.Panicln("Unable to read configuration file: ", err.Error())
	}

	log.Printf("Configuration:")
	log.Printf("Results: %v\n", jsontype)
	log.Printf("Host : %v\n", jsontype.Database.Host)
	log.Printf("Tables : %v\n", jsontype.Database.TablesDesc)
	return jsontype
}
