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
		result, err := executeQuery(db, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "the query has been succesfully executed: %v\n", result)
	}
}

func decodeQuery(queryType string, requestBody io.ReadCloser) (interface{}, error) {
	switch queryType {
	case "create_table":
		var query CreateTableQuery
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
	case "update":
		var query UpdateQuery
		err := decode(&query, requestBody)

		return query, err
	case "delete":
		var query DeleteQuery
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

func executeQuery(db *Database, q interface{}) (interface{}, error) {
	switch query := q.(type) {
	case CreateTableQuery:
		return nil, db.CreateTable(query)
	case SelectQuery:
		return db.Select(query)	
	case InsertQuery:
		return db.Insert(query)	
	case UpdateQuery: 
		return db.Update(query)
	case DeleteQuery: 
		return db.Delete(query)
	default:
		return nil, fmt.Errorf("unsupported query type: %T", query)
	}
}
