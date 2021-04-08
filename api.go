package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func handler(db *Database) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		queryType := r.Header.Get("x-query-type")
		query, err := decodeQuery(queryType, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("executing query: %s\n", query)
		err = executeQuery(db, query)
		log.Printf("execution result err=%v\n", err)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintln(w, "the query has been succesfully executed")
	}
}

func decodeQuery(queryType string, requestBody io.ReadCloser) (interface{}, error) {
	switch queryType {
	case "create_table":
		var query InsertQuery
		err := decode(&query, requestBody)

		return query, err
	case "insert":
		var query InsertQuery
		err := decode(&query, requestBody)

		return query, err
	case "select":
		var query SelectQuery
		err := decode(&query, requestBody)

		return query, err
	default:
		return nil, fmt.Errorf("unsupported query type %s", queryType)
	}
}

func decode(query interface{}, requestBody io.ReadCloser) error {
	err := json.NewDecoder(requestBody).Decode(query)
	if err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func executeQuery(db *Database, q interface{}) error {
	switch query := q.(type) {
	case CreateTableQuery:
		return db.CreateTable(query)
	case InsertQuery:
		return db.Insert(query)
	case SelectQuery:
		return db.Select(query)
	default:
		return fmt.Errorf("unsupported query type: %T", query)
	}
}
