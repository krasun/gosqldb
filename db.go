package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
)

// regular expressions to check table and column names
var entityNameRegExp = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var tableNameRegExp = entityNameRegExp
var isValidTableNameFormat = entityNameRegExp.MatchString
var columnNameRegExp = entityNameRegExp
var isValidColumnNameFormat = entityNameRegExp.MatchString

// name of the meta file that stores information about
// table structures and other database meta information
const metaFileName = "gosqldb.meta.json"

// table file extension
const tableFileExtension = ".table.json"

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
	tables map[string]Schema
	// data by table name
	data map[string][][]interface{}
}

// Schema represents a database table schema.
type Schema struct {
	Name    string               `json:"name"`
	Columns map[string]ColumnDef `json:"columns"`
}

// ColumnDef describes a table column.
type ColumnDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
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

	tables, err := loadSchema(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load tables: %w", err)
	}

	tableData, err := loadData(dbDir, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	return &Database{
		dbDir,
		metaFilePath,
		tables,
		tableData,
	}, nil
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

	tableColumns := make(map[string]ColumnDef)

	// to detect column definition duplicates
	columnNames := make(map[string]struct{})
	for columnPosition, column := range query.Columns {
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

		tableColumns[columnName] = ColumnDef{Name: columnName, Type: columnType, Position: columnPosition}
	}

	table := Schema{Name: tableName, Columns: tableColumns}

	db.tables[tableName] = table
	err := storeSchema(db.metaFilePath, db.tables)
	if err != nil {
		return fmt.Errorf("failed to store tables: %w", err)
	}

	return nil
}

// Select fetches data from the database.
func (db *Database) Select(query SelectQuery) ([][]interface{}, error) {
	tableName := strings.ToLower(query.From)
	tableData, exists := db.data[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	schema := db.tables[tableName]

	// // @todo validate select query

	matched := make([][]interface{}, 0)
	for _, row := range tableData {
		if matches(schema, row, query.Where) {
			matched = append(matched, row)
		}
	}

	fmt.Println(matched)

	return matched, nil
}

func matches(schema Schema, row []interface{}, exprs []WhereExpression) bool {
	for _, expr := range exprs {
		if !exprMatch(schema, row, expr) {
			return false
		}
	}

	return true
}

func exprMatch(schema Schema, row []interface{}, expr WhereExpression) bool {
	left := extractVal(schema, row, expr.Left)
	right := extractVal(schema, row, expr.Right)

	return right == left
}

func extractVal(schema Schema, row []interface{}, operand Operand) interface{} {
	if operand.Type == "value" {
		return operand.Value
	}

	// identifier
	column := operand.Value.(string)
	p := schema.Columns[column].Position

	return row[p]
}

// Insert inserts data into the database.
func (db *Database) Insert(query InsertQuery) error {
	tableName := strings.ToLower(query.TableName)
	table, exists := db.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if len(query.Values) == 0 {
		return fmt.Errorf("empty values, at least one is required")
	}

	var insertColumns = make(map[string]int)
	for index, column := range query.Columns {
		columnName := strings.ToLower(column)
		if _, exists := table.Columns[columnName]; !exists {
			return fmt.Errorf("column %s does not exist in table %s", column, tableName)
		}

		insertColumns[columnName] = index
	}

	for _, requiredColumn := range table.Columns {
		if _, exists := insertColumns[requiredColumn.Name]; !exists {
			return fmt.Errorf("%s column value is not provided", requiredColumn.Name)
		}
	}

	for row, values := range query.Values {
		if len(values) != len(query.Columns) {
			return fmt.Errorf("the number of values must be equal to the number of columns at row %d", row)
		}
	}

	newRows := sortValues(table, insertColumns, query.Values)
	err := db.writeToFile(tableName, newRows)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}
	log.Printf("the record has been inserted succesfully into %s", tableName)

	// store the data in-memory
	db.data[tableName] = append(db.data[tableName], newRows...)

	return nil
}

func (db *Database) writeToFile(tableName string, newRows [][]interface{}) error {
	tableFilePath := tableFilePath(db.dbDir, tableName)
	data, err := ioutil.ReadFile(tableFilePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read file %s: %w", tableFilePath, err)
	}

	var rows [][]interface{}
	var file *os.File
	defer func() {
		if file != nil {
			checkFileClose(tableFilePath, file.Close())
		}
	}()

	if os.IsNotExist(err) {
		rows = make([][]interface{}, 0)
	} else {
		err := json.Unmarshal(data, &rows)
		if err != nil {
			return fmt.Errorf("failed to decode JSON from %s: %w", tableFilePath, err)
		}
	}

	file, err = os.Create(tableFilePath)
	if err != nil {
		return fmt.Errorf("failed to create/open file for write %s: %w", tableFilePath, err)
	}

	rows = append(rows, newRows...)

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")

	err = encoder.Encode(rows)
	if err != nil {
		return fmt.Errorf("failed to encode JSON and write to file for %s: %w", tableFilePath, err)
	}

	return nil
}

// Update updates data in the database.
func (db *Database) Update(query UpdateQuery) {

}

// Delete deletes data from the database.
func (db *Database) Delete(query DeleteQuery) {

}

func tableFilePath(dbDir string, tableName string) string {
	return path.Join(dbDir, tableName) + tableFileExtension
}

func sortValues(table Schema, insertColumns map[string]int, values [][]interface{}) [][]interface{} {
	newRows := make([][]interface{}, len(values))
	for rowIndex, row := range values {
		newRow := make([]interface{}, len(row))

		for columnName, index := range insertColumns {
			position := table.Columns[columnName].Position
			newRow[position] = row[index]
		}

		newRows[rowIndex] = newRow
	}

	return newRows
}

func initializeMetaFile(metaFilePath string) error {
	_, err := os.Stat(metaFilePath)
	if err == nil {
		log.Printf("meta file %s has been already initialized\n", metaFilePath)
		return nil
	}

	if os.IsNotExist(err) {
		log.Printf("meta file %s does not exist, creating a new one...\n", metaFilePath)
		err = storeSchema(metaFilePath, make(map[string]Schema))
		if err != nil {
			return fmt.Errorf("failed to store empty table map to %s: %w", metaFilePath, err)
		}

		return nil
	}

	return fmt.Errorf("failed to read information about %s: %w", metaFilePath, err)
}

func loadSchema(metaFilePath string) (map[string]Schema, error) {
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", metaFilePath, err)
	}
	defer func() { checkFileClose(metaFilePath, metaFile.Close()) }()

	var tables map[string]Schema

	decoder := json.NewDecoder(metaFile)
	err = decoder.Decode(&tables)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON from %s: %w", metaFilePath, err)
	}

	return tables, nil
}

func storeSchema(metaFilePath string, tables map[string]Schema) error {
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

func loadData(dbDir string, tables map[string]Schema) (map[string][][]interface{}, error) {
	tableData := make(map[string][][]interface{}, 0)
	for tableName, _ := range tables {
		tableFilePath := tableFilePath(dbDir, tableName)

		data, err := ioutil.ReadFile(tableFilePath)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read file %s: %w", tableFilePath, err)
		}

		var rows [][]interface{}
		if os.IsNotExist(err) {
			rows = make([][]interface{}, 0)
		} else {
			err = json.Unmarshal(data, &rows)
			if err != nil {
				return nil, fmt.Errorf("failed to decode JSON from %s: %w", tableFilePath, err)
			}
		}

		tableData[tableName] = rows
	}

	return tableData, nil
}

func checkFileClose(filePath string, err error) {
	if err != nil {
		panic(fmt.Errorf("failed to close file %s: %w", filePath, err))
	}
}
