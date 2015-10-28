package db

import (
	"bytes"
	"encoding/binary"
	"log"
	
)

type Column interface {
	GetName() string
	GetSqlType() SqlType
	generateValue() interface{}
	String() string
}

type SimpleColumn struct {
	name string
	sqlType SqlType
}

type BlobColumn struct {
	SimpleColumn
	size int
}

func (column SimpleColumn) String() string {
	return column.name+" "+column.sqlType.String()
}

func (column BlobColumn) String() string {
	return column.name+" "+BLOB.String()
}

func (column SimpleColumn) GetName() string {
	return column.name
}


func (column SimpleColumn) GetSqlType() SqlType {
	return column.sqlType
}

func (column SimpleColumn) generateValue() interface{} {
	return column.sqlType.rand()
}

func (column BlobColumn) generateValue() interface{} {
	buf := new(bytes.Buffer)

	for i := 0; i < column.size-1; i++ {
		err := binary.Write(buf, binary.LittleEndian, column.sqlType.rand())
		if err != nil {
			log.Fatal("binary.Write failed:", err)
		}
	}
	//log.Printf("%x", buf.Bytes())
	return buf.Bytes()
}













