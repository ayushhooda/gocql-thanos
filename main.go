package main

import (
	"container/list"
	"fmt"
	"github.com/gocql/gocql"
)

/**
Code to fetch metadata of a table using gocql -> golang cassandra package
*/

func main() {
	fmt.Println("running")

	cluster := gocql.NewCluster("127.0.0.1:9042")

	cluster.Keyspace = "system_schema"

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	session, err := cluster.CreateSession()

	l := list.New()

	if err == nil {
		cqlQuery := "SELECT * FROM system_schema.columns WHERE keyspace_name = 'netq' and table_name = 'procnetdev'"

		iter := session.Query(cqlQuery).Iter()

		var (
			keyspaceName    string
			tableName       string
			columnName      string
			clusteringOrder string
			columnNameBytes string
			kind            string
			position        string
			dataType        string
		)

		for iter.Scan(&keyspaceName, &tableName, &columnName, &clusteringOrder, &columnNameBytes, &kind, &position, &dataType) {
			fmt.Println("Inside iterator", columnName)
			l.PushBack(columnName)
		}

		fmt.Println(iter.NumRows())

	} else {
		fmt.Println("unable to make connection")
	}

	fmt.Println("Column list", l.Len())

}
