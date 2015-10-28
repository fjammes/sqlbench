package main

import (
	"flag"
	"fmt"
	"log"
	"lsstcorp.org/db"
	"os"
)

var configvar string
var dbvar string
var portvar int

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s: [options] \n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func init() {
	flag.StringVar(&configvar, "config", "./config.json", "Path to JSON configuration file")
	flag.StringVar(&dbvar, "db", "", "Database name (overload JSON value)")
	flag.IntVar(&portvar, "port", -1, "Port number (overload JSON value)")
}

func main() {

	var (
		database     db.Database
		connection   db.Connection
		databaseDesc db.DatabaseDesc
	)

	flag.Usage = usage
	flag.Parse()

	json := db.ReadConfig(configvar)

	databaseDesc = json.Database

	if len(dbvar) != 0 {
		databaseDesc.Name = dbvar
	}

	if portvar != -1 {
		databaseDesc.Port = portvar
	}

	// flag.Usage()

	log.Printf("Open db connection %s", databaseDesc)
	connection.Open(databaseDesc.User,
		databaseDesc.Pass,
		databaseDesc.Host,
		databaseDesc.Port,
		databaseDesc.Name)

	log.Printf("Querying data")
	database.PrepareQueries()
	database.RunQueries(&connection)

}
