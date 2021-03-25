package main

import "log"

func main() {
	// @todo acquire os level lock, to make sure that only process works with
	// db files

	path := "testdata"
	db, err := NewDatabase(path)
	if err != nil {
		log.Fatalf("failed to instantiate database: %s", err)
	}

	query := CreateTableQuery{
		TableName: "planets",
		Columns: []struct {
			Name string
			Type string
		}{
			{"id", "integer"},
			{"name", "string"},
		}}
	err = db.CreateTable(query)
	if err != nil {
		log.Fatalf("failed to create table: %s", err)
	}
}
