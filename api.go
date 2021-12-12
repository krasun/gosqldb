package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	sql "github.com/krasun/gosqlparser"
)

func handler(db *Database) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := parseQuery(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("executing query: %s\n", query)
		result, err := executeQuery(db, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "the query has been successfully executed: %v\n", result)
	}
}

func parseQuery(requestBody io.ReadCloser) (sql.Statement, error) {
	body, err := ioutil.ReadAll(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	query, err := sql.Parse(string(body))
	if err != nil {
		return nil, fmt.Errorf("failed to parse body: %w", err)
	}

	return query, nil
}

func executeQuery(db *Database, q sql.Statement) (interface{}, error) {
	switch query := q.(type) {
	case *sql.CreateTable:
		return nil, db.CreateTable(query)
	case *sql.DropTable:
		return nil, db.DropTable(query)
	case *sql.Select:
		return db.Select(query)
	case *sql.Insert:
		return db.Insert(query)
	case *sql.Update:
		return db.Update(query)
	case *sql.Delete:
		return db.Delete(query)
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}
}
