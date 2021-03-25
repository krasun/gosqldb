package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
)

var columnTypes = map[string]struct{}{
	"integer": {},
	"string":  {},
}

// regular expressions to check table and column names
var entityNameRegExp = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var tableNameRegExp = entityNameRegExp
var isValidTableNameFormat = entityNameRegExp.MatchString
var columnNameRegExp = entityNameRegExp
var isValidColumnNameFormat = entityNameRegExp.MatchString

// name of the meta file that stores information about
// table structures and other database meta information
const metaFileName = "gosqldb.meta"

// Database is an orchestractor and main entry point for working
// with a database.
type Database struct {
	// a dbDir to the directory where the database stores
	// all the data
	dbDir string
	// path to the meta file that stores information about
	// table structures and other database meta information
	metaFilePath string
	// pointers to the tables
	// by lowercase table names
	tables map[string]Table
}

// Table represents a database table.
type Table struct {
	Name    string                 `json:"name"`
	Columns map[string]TableColumn `json:"columns"`
}

// TableColumn describes a table column.
type TableColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// NewDatabase creates new instance of the database and loads
// all the necessary information.
func NewDatabase(dbDir string) (*Database, error) {
	dbDirStat, err := os.Stat(dbDir)
	if err != nil && os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read directory %s: %w", dbDir, err)
	}

	if !dbDirStat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory %s", dbDir, err)
	}

	metaFilePath := path.Join(dbDir, metaFileName)
	err = initializeMetaFile(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize meta file %s: %w", metaFilePath, err)
	}

	tables, err := loadTables(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load tables: %w", err)
	}

	return &Database{
		dbDir,
		metaFilePath,
		tables,
	}, nil
}

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

func checkFileClose(filePath string, err error) {
	if err != nil {
		panic(fmt.Errorf("failed to close file %s: %w", filePath, err))
	}
}

// CreateTable creates a table.
func (db *Database) CreateTable(query CreateTableQuery) error {
	tableName := strings.ToLower(query.TableName)
	if len(tableName) == 0 {
		return fmt.Errorf("table name is empty")
	}

	if !isValidTableNameFormat(tableName) {
		return fmt.Errorf("table name %s is not valid, expected format: %s", query.TableName, tableNameRegExp)
	}

	_, exists := db.tables[tableName]
	if exists {
		return fmt.Errorf("table %s exists (table names are case-insensitive)", query.TableName)
	}

	if len(query.Columns) == 0 {
		return fmt.Errorf("failed to create %s: table must have at least one column", query.TableName)
	}

	tableColumns := make(map[string]TableColumn)

	// to detect column definition duplicates
	columnNames := make(map[string]struct{})
	for _, column := range query.Columns {
		columnName := strings.ToLower(column.Name)
		if len(columnName) == 0 {
			return fmt.Errorf("column name is empty for table %s", query.TableName)
		}

		if !isValidColumnNameFormat(tableName) {
			return fmt.Errorf("column name %s is not valid, expected format: %s", column.Name, columnNameRegExp)
		}

		if _, exists := columnNames[columnName]; exists {
			return fmt.Errorf("%s definition is repeated (column names are case-insensitive)", column.Name)
		}

		columnType := strings.ToLower(column.Type)
		if _, exists := columnTypes[columnType]; !exists {
			return fmt.Errorf("%s type definition is not found for column %s", column.Type, column.Name)
		}

		columnNames[columnName] = struct{}{}

		tableColumns[columnName] = TableColumn{Name: columnName, Type: columnType}
	}

	table := Table{Name: tableName, Columns: tableColumns}

	db.tables[tableName] = table
	err := storeTables(db.metaFilePath, db.tables)
	if err != nil {
		return fmt.Errorf("failed to store tables: %w", err)
	}

	return nil
}

// RenameTable creates renames a table.
func (db *Database) RenameTable(query RenameTableQuery) {

}

// DropTable drops a table.
func (db *Database) DropTable(query DropTableQuery) {

}

// AddColumn creates adds a column.
func (db *Database) AddColumn(query AddColumnQuery) {

}

// RenameColumn creates renames a column.
func (db *Database) RenameColumn(query RenameColumnQuery) {

}

// DropColumn changes a column type.
func (db *Database) ChangeColumnType(query ChangeColumnTypeQuery) {

}

// DropColumn drops a column.
func (db *Database) DropColumn(query DropColumnQuery) {

}

// Select fetches data from the database.
func (db *Database) Select(query SelectQuery) {

}

// Insert inserts data into the database.
func (db *Database) Insert(query InsertQuery) {

}

// Update updates data in the database.
func (db *Database) Update(query UpdateQuery) {

}

// Delete deletes data from the database.
func (db *Database) Delete(query DeleteQuery) {

}
