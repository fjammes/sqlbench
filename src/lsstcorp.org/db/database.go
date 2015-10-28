package db

import (
	"log"
)

const (
	SIMPLE = false
	CLUSTER = true
)

func ModeCast(mode string) bool {
	switch {
	case mode  == "SIMPLE":
		return SIMPLE
	case mode == "CLUSTER":
		return CLUSTER
	default:
		log.Fatalf("%v doesn't exist",mode)
	}
	return SIMPLE
}

type Database struct  {
	name    string
	sizeGB  int
	tables  []Table
	queries []string
}

func (database *Database) CreateTables(connection *Connection) {
	for _, tbl := range database.tables {
		connection.CreateTable(tbl)
	}
}

func (database *Database) Empty(connection *Connection) {
		connection.Empty(database)
}

func (database *Database) Fill(connection *Connection, sizeGB float32) {
		connection.FillTables(database, sizeGB)
}

func (database *Database) GetTablesNames() []string {
	tablesNames :=  make([]string,  len(database.tables))
	for i, table := range database.tables {
		tablesNames[i] = table.name 	
	}
	return tablesNames
}

func (database *Database) PrepareQueries() {
	log.Printf("Preparing queries for %v", database.name)

	// QUERY 0
	sql := "SELECT count(*) as COUNT FROM "+database.tables[0].name+";"
	database.queries = append(database.queries, sql)

	// QUERY 1
	selectStr:="SELECT "+database.tables[0].name+"."+database.tables[0].columns[0].GetName()
	fromStr:="FROM "+database.tables[0].name

	sql = selectStr+" "+fromStr+" WHERE id=500;"
	database.queries = append(database.queries, sql)

	// QUERY 2
	selectStr="SELECT "+database.tables[0].name+"."+database.tables[0].columns[0].GetName()

	fromStr="FROM "+database.tables[0].name
	joinStr:=""
	precTable:=database.tables[0]
	for _, table := range database.tables[1:] {
		joinStr +=  " AND "+precTable.name+".id = "+table.name+".id"
		fromStr += ", "+table.name
		selectStr += ", "+table.name+"."+table.columns[0].GetName()
		precTable=table
	}

	sql = selectStr+" "+fromStr+" WHERE "+database.tables[0].name+".id=500"+joinStr
	database.queries = append(database.queries, sql)

	// QUERY 3
	/*
	SELECT v.objectId, v.ra, v.decl
        FROM   Object v, Object o
        WHERE  o.objectId = :objectId
        AND spDist(v.ra, v.decl, o.ra, o.decl, :dist)
        AND v.variability > 0.8
        AND o.extendedParameter > 0.8
        */
	columnNames := database.getSimpleColumns(0,DOUBLE) 

	if len(columnNames) != 0 {

		selectStr="SELECT "+database.tables[0].name+".id, "+columnNames[0]
		
		fromStr="FROM "+database.tables[0].name
		joinStr=""
		precTable=database.tables[0]
		for _, table := range database.tables[1:] {
			joinStr +=  " AND "+precTable.name+".id = "+table.name+".id"
			fromStr += ", "+table.name
			selectStr += ", "+table.name+"."+table.columns[0].GetName()
			precTable=table
		}
		
		sql = selectStr+" "+fromStr+" WHERE "+columnNames[0]+">0.99 "+joinStr
		database.queries = append(database.queries, sql)
	} else {
		log.Printf("WARNING : Unable to prepare query 3 : no float columns in tables")
	}
}

func (database *Database) RunQueries(connection *Connection) {
	log.Printf("GNUPLOT output |# %v (query n°, time in sec)", database.name)
	connection.RunQueries(database)
}

func (database *Database) getSimpleColumns(tableIndex int, sqlType SqlType) []string {
	var result []string
	table := database.tables[tableIndex]
	for j, column := range table.columns {
		switch t := column.(type) {
		default:
			log.Fatalf("Unexpected type %T", t)
		case SimpleColumn:
			if column.GetSqlType() == sqlType {
				result = append(result,table.name+"."+table.columns[j].GetName())
			}
		case BlobColumn:
			log.Printf("BLOB %T\n", t)
		}
	}
	return result
}

















