package db

import (
	"log"
	"fmt"
)

type Table struct {
	name    string
	rowSizeBits int
	columns []Column
}

func (table *Table) addColumn(sqlType SqlType) {
	//colName := fmt.Sprintf("%v_%v",sqlType,len(table.columns))
	colName := fmt.Sprintf("%v%v",sqlType.String()[:1],len(table.columns))
	//log.Printf("Adding column %s", colName)
	table.columns = append(table.columns,SimpleColumn{colName,sqlType})
	table.rowSizeBits += sqlType.Size()
	//log.Printf("Table %s", table.columns)
}

func (table *Table) addColumnCluster(size int, sqlType SqlType) {
	colName := fmt.Sprintf("%v_%v_cluster%v_%v",BLOB,len(table.columns),size,sqlType)
	table.columns = append(table.columns, BlobColumn{SimpleColumn{colName,sqlType},size})
	table.rowSizeBits += size * sqlType.Size()
}


/* TODO : move to table.go */
func (table *Table) AddColumns(nbCol int, sqlType SqlType, columnType bool) {
	cluster_msg := ""
	if columnType == SIMPLE {
		for i := 0; i < nbCol; i++ {
			table.addColumn(sqlType)
		}	
	} else {
		cluster_msg="(BLOB CLUSTER)"
		table.addColumnCluster(nbCol,sqlType)
	}
	log.Printf("  + adding %v \"%v\" columns %v", nbCol, sqlType, cluster_msg)
	log.Printf("    row size : %v bytes", 	table.rowSizeBits)
}


















