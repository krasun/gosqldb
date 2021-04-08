package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func initializeMetaFile(metaFilePath string) error {
	_, err := os.Stat(metaFilePath)
	if err == nil {
		log.Printf("meta file %s has been already initialized\n", metaFilePath)
		return nil
	}

	if os.IsNotExist(err) {
		log.Printf("meta file %s does not exist, creating a new one...\n", metaFilePath)
		err = storeTables(metaFilePath, make(map[string]Table))
		if err != nil {
			return fmt.Errorf("failed to store empty table map to %s: %w", metaFilePath, err)
		}

		return nil
	}

	return fmt.Errorf("failed to read information about %s: %w", metaFilePath, err)
}

func loadTables(metaFilePath string) (map[string]Table, error) {
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", metaFilePath, err)
	}
	defer func() { checkFileClose(metaFilePath, metaFile.Close()) }()

	var tables map[string]Table

	decoder := json.NewDecoder(metaFile)
	err = decoder.Decode(&tables)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON from %s: %w", metaFilePath, err)
	}

	return tables, nil
}

func storeTables(metaFilePath string, tables map[string]Table) error {
	metaFile, err := os.Create(metaFilePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", metaFilePath, err)
	}
	defer func() { checkFileClose(metaFilePath, metaFile.Close()) }()

	encoder := json.NewEncoder(metaFile)
	encoder.SetIndent("", "\t")

	err = encoder.Encode(tables)
	if err != nil {
		return fmt.Errorf("failed to encode JSON for %s: %w", metaFilePath, err)
	}

	return nil
}

func loadTableData(dbDir string, tables map[string]Table) (map[string][][]interface{}, error) {
	// for tableName, table := range tables {

	// }

	return nil, nil
}

func checkFileClose(filePath string, err error) {
	if err != nil {
		panic(fmt.Errorf("failed to close file %s: %w", filePath, err))
	}
}
