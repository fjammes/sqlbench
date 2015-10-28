package db

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"log"
	"bytes"
	"time"
)

type Connection struct {
	dsn string
	netAddr string
	available bool
	db *sql.DB
}

func (connection *Connection) Open(user string, pass string, host string, port int,  dbname string) {
	prot := "tcp"
	addr := fmt.Sprintf("%s:%v", host, port)
	netAddr := fmt.Sprintf("%s(%s)", prot, addr)
        if len(pass) > 0 {
            pass = fmt.Sprintf(":%s", pass)
        }
	log.Printf("Connection params: %s, %s, %s, %s",  user, pass, netAddr, dbname)
        connection.dsn = fmt.Sprintf("%s%s@%s/%s?timeout=30s&strict=true", user, pass, netAddr, dbname)
	log.Printf("Connection dsn: %s", connection.dsn)
	db, err := sql.Open("mysql", connection.dsn)
	if err != nil {
		log.Fatal("Error connecting: %s", err.Error())
	}
	//defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	
	connection.db = db
	err = connection.db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
}

func (connection *Connection) Exec(query string) {
	//log.Println("Launching query : ", query)
	_, err := connection.db.Exec(query); 
	if err != nil {
		// continue on error
		log.Printf("Error on %s: %s", query, err.Error())
	}
}

func (connection *Connection) ExecStmtInsert(sql string, args ...interface{}) {
	
	//log.Printf("Launching query %s with %v ", sql, args)
	stmtIns, err := connection.db.Prepare(sql) // ? = placeholder
	if err != nil {
		log.Panicln("Prepared statement error : ", err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(args...) // Insert tuples (i, i^2)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
}

func (connection *Connection) mustExec(query string, args ...interface{}) (res sql.Result) {
	//log.Println("Launching query : ", query)
	res, err := connection.db.Exec(query, args...)
	if err != nil {
		connection.fail("Exec", query, err)
	}
	return res
}


func (connection *Connection) mustQuery(query string, args ...interface{}) (res *sql.Rows) {
	//log.Println("Launching query : ", query)
	rows, err := connection.db.Query(query, args...)
	if err != nil {
		connection.fail("Exec", query, err)
	}
	return rows
}


func (connection *Connection) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	log.Fatalf("Error on %s %s: %s", method, query, err.Error())
}

func (connection *Connection) CreateTable(table Table) {
	var buffer bytes.Buffer
	log.Printf("Creating table %v", table.name)
	buffer.WriteString("CREATE TABLE "+table.name+" (")
	buffer.WriteString("id BIGINT NOT NULL, ")
	for _, col := range table.columns {
		buffer.WriteString("  "+col.String()+", ")
	}
	buffer.WriteString("PRIMARY KEY (id)) ENGINE=MyISAM DEFAULT CHARSET=latin1;")
	connection.mustExec(buffer.String())
}

func (connection *Connection) Empty(database *Database) {
	log.Printf("Emptying database %v", database.name)
	connection.mustExec("SET @tables = NULL;")
	connection.mustExec("SELECT GROUP_CONCAT(table_schema, '.', table_name) INTO @tables FROM information_schema.tables WHERE table_schema = ?;", database.name)
	connection.mustExec("SET @tables = IFNULL(CONCAT('DROP TABLE ', @tables),'SELECT 1');")
	connection.mustExec("PREPARE stmt FROM @tables")
	connection.mustExec("EXECUTE stmt;")
	connection.mustExec("DEALLOCATE PREPARE stmt;")

}


func (connection *Connection) FillTables(database *Database, dataSize  float32) {
	var insertedDataSize float32
	var theoricDataSize float32 
	var pStatements []string

	pStatements = prepareStatement(database)
	
	log.Printf("Inserting around %v GB of random data in tables : %v", dataSize, database.GetTablesNames())
	connection.disableKeys(database)
	insertedDataSize=0
	row:=0
	for insertedDataSize < dataSize {
		if ((row+1)%1000 == 0) {
			theoricDataSize, insertedDataSize = connection.getDataSize(database, row)
			log.Printf("  + %v row inserted", row+1)
			log.Printf("    Inserted data size : %v GB (theoric : %v GB)", insertedDataSize, theoricDataSize)
		}
		for i, table := range database.tables {
			
			values := make([]interface{},  len(table.columns)+1)
			// Primary key management
			values[0]=row
			for j, col := range table.columns {
				values[j+1]=interface{}(col.generateValue())
			}
			connection.ExecStmtInsert(pStatements[i], values...)
		}
		row++
	}
	connection.enableKeys(database)
}

func (connection *Connection) getDataSize(database *Database, row int) (theoricDataSize float32, realDataSize float32) {
	var rows *sql.Rows
	theoricDataSize = 0
	for _, table := range database.tables {
		theoricDataSize += float32(table.rowSizeBits * (row+1))
		//log.Printf("Inserted data size : %v GB", dataSize)
	}
	theoricDataSize /= (8 * 1000000000)

	query := "SELECT SUM(round(((data_length + index_length) / 1024 / 1024), 2))/1000 FROM information_schema.TABLES  WHERE table_schema = ?  AND table_name like \""+TABLE_PREFIX+"%\""
	rows = connection.mustQuery(query, database.name)
	if rows.Next() {
		rows.Scan(&realDataSize)
		if rows.Next() {
			log.Printf("DATASIZE: unexpexted row")
		}
	} else {
		log.Fatalf("DATASIZE: no data")
	}

	return
}

func prepareStatement(database *Database) []string {
	var buffer bytes.Buffer
	pStatements := make([]string,  len(database.tables))
	for i, table := range database.tables {
		buffer.Reset()
		buffer.WriteString("INSERT INTO " + table.name + " VALUES (")
		for _=range(table.columns) {
			buffer.WriteString("?, ")
		}
		buffer.WriteString("?)")
		pStatements[i] = buffer.String()
	}
	return pStatements	
}

func (connection *Connection) disableKeys(database *Database) {
	for _, table := range database.tables {
		connection.mustExec("ALTER TABLE "+table.name+" DISABLE KEYS;")
	}	
}

func (connection *Connection) enableKeys(database *Database) {
	for _, table := range database.tables {
		connection.mustExec("ALTER TABLE "+table.name+" ENABLE KEYS;")
	}	
}




func (connection *Connection) RunQueries(database *Database) {
	var rows *sql.Rows
	for k, query := range database.queries {
		log.Printf("LAUNCHING QUERY %v : %s", k, query)
		t0 := time.Now()
		rows = connection.mustQuery(query)
		t1 := time.Now()
		d := t1.Sub(t0)
		log.Printf("The query %v call took %v to run", k, d)
		log.Printf("GNUPLOT output |%v %v", k, time.Duration(d).Seconds())
		
		columns, err := rows.Columns()
		if err != nil {
			log.Fatalf(err.Error()) // proper error handling instead of panic in your app
		}
		
		log.Printf("Columns : %v",columns)
		
		// Make a slice for the values
		values := make([]sql.RawBytes, len(columns))
		
		// rows.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		
		// Fetch rows
		for i:=0;rows.Next() && i<5;i++ {
			// get RawBytes from data
			err = rows.Scan(scanArgs...)
			if err != nil {
				log.Panicf(err.Error()) // proper error handling instead of panic in your app
			}
			
			// Now do something with the data.
			// Here we just print each column as a string.
			var value string
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					value = "NULL"
				} else if len(col) < 100 {
					value = string(col)
				} else {
					value = "Too long"
				}
				log.Println(columns[i], ": ", value)
			}
			log.Println("-----------------------------------")	
		}
	}

	return
}







