package db

import (
	"math/rand"
	"log"
)

type SqlType int

const (
	BIGINT SqlType = iota
	FLOAT
	DOUBLE
	BLOB
	UNDEFINED
)

func (sqlType SqlType) String() string {
	switch {
	case sqlType == BIGINT:
		return "BIGINT"
	case sqlType == FLOAT:
		return "FLOAT"
	case sqlType == DOUBLE:
		return "DOUBLE"
	case sqlType == BLOB:
		return "BLOB"
	}
	return "UNDEFINED"
}

func SqlTypeCast(sqlTypeStr string) SqlType {
	switch {
	case sqlTypeStr  == "BIGINT":
		return BIGINT
	case sqlTypeStr == "FLOAT":
		return FLOAT
	case sqlTypeStr == "DOUBLE":
		return DOUBLE
	case sqlTypeStr == "BLOB":
		return BLOB
	default:
		log.Fatalf("%v doesn't exist",sqlTypeStr)
	}
	return UNDEFINED
	
}

func (sqlType SqlType) Size() int {
	switch {
	case sqlType == BIGINT ||  sqlType == DOUBLE:
		return 64
	case sqlType == FLOAT:
		return 32
	}
	return -1
}

func (sqlType SqlType) rand() interface{} {
	switch {
	case sqlType == BIGINT:
		return rand.Int63()
	case sqlType == DOUBLE:
		return rand.Float64()
	case sqlType == FLOAT:
		return float32(rand.Float64())
	case sqlType == BLOB:
		return "blob"
	}
	return "Not randomizable value"
}