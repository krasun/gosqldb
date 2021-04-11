package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"reflect"
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

func (def ColumnDef) ReflectType() reflect.Type {
	switch def.Type {
	case "integer":
		return reflect.TypeOf(0)
	case "string":
		return reflect.TypeOf("")
	}

	return nil
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
	schema, exists := db.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	err := validateWhereExpr(schema, query.Where)
	if err != nil {
		return nil, fmt.Errorf("invalid WHERE part: %w", err)
	}

	tableData := db.data[tableName]
	matched := make([][]interface{}, 0)
	for _, row := range tableData {
		if matches(schema, row, query.Where) {
			matched = append(matched, row)
		}
	}

	return matched, nil
}

func validateWhereExpr(schema Schema, whereExprs []WhereExpression) error {
	for i, expr := range whereExprs {
		lt, err := validateOperand(schema, expr.Left)
		if err != nil {
			return fmt.Errorf("invalid left operand at %d: %w", i, err)
		}

		rt, err := validateOperand(schema, expr.Right)
		if err != nil {
			return fmt.Errorf("invalid right operand at %d: %w", i, err)
		}

		if rt != lt {
			return fmt.Errorf("operand types do not match: %s != %s", lt, rt)
		}

		err = validateOperation(expr.Operation)
		if err != nil {
			return fmt.Errorf("invalid operation at %d: %w", i, err)
		}
	}

	return nil
}

func validateOperation(op string) error {
	switch op {
	case "eq":
		return nil
	default:
		return fmt.Errorf("unsupported operation: %s", op)
	}
}

func validateOperand(schema Schema, operand Operand) (reflect.Type, error) {
	operandType := strings.ToLower(operand.Type)
	switch operandType {
	case "value":
		return valueType(operand.Value), nil
	case "identifier":
		val, ok := operand.Value.(string)
		if !ok {
			return nil, fmt.Errorf("identifier %v is not a string", operand.Value)
		}

		column := strings.ToLower(val)
		_, exists := schema.Columns[column]
		if !exists {
			return nil, fmt.Errorf("column %s does not exist", column)
		}

		return schema.Columns[column].ReflectType(), nil
	default:
		return nil, fmt.Errorf("unsupported operand type %s", operand.Type)
	}
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
func (db *Database) Insert(query InsertQuery) (int, error) {
	tableName := strings.ToLower(query.TableName)
	table, exists := db.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}

	if len(query.Values) == 0 {
		return 0, fmt.Errorf("empty values, at least one is required")
	}

	var insertColumns = make(map[string]int)
	for index, column := range query.Columns {
		columnName := strings.ToLower(column)
		if _, exists := table.Columns[columnName]; !exists {
			return 0, fmt.Errorf("column %s does not exist in table %s", column, tableName)
		}

		insertColumns[columnName] = index
	}

	for _, requiredColumn := range table.Columns {
		if _, exists := insertColumns[requiredColumn.Name]; !exists {
			return 0, fmt.Errorf("%s column value is not provided", requiredColumn.Name)
		}
	}

	for row, values := range query.Values {
		if len(values) != len(query.Columns) {
			return 0, fmt.Errorf("the number of values must be equal to the number of columns at row %d", row)
		}
	}

	newRows := sortValues(table, insertColumns, query.Values)
	err := db.writeToFileNewRows(tableName, newRows)
	if err != nil {
		return 0, fmt.Errorf("failed to write to file: %w", err)
	}
	log.Printf("the record has been inserted succesfully into %s", tableName)

	// store the data in-memory
	db.data[tableName] = append(db.data[tableName], newRows...)

	return len(newRows), nil
}

// Update updates data in the database.
func (db *Database) Update(query UpdateQuery) (int, error) {
	tableName := strings.ToLower(query.TableName)
	schema, exists := db.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}

	err := validateWhereExpr(schema, query.Where)
	if err != nil {
		return 0, fmt.Errorf("invalid WHERE part: %w", err)
	}

	err = validateExpr(schema, query.Set)
	if err != nil {
		return 0, fmt.Errorf("invalid SET part: %w", err)
	}

	tableData := db.data[tableName]
	updCnt := 0
	updateRows := make(map[int][]interface{})
	for index, row := range tableData {
		if matches(schema, row, query.Where) {
			updateRows[index] = updateValues(schema, query.Set, row)
			updCnt++
		}
	}

	err = db.updateRowsInFile(tableName, updateRows)
	if err != nil {
		return 0, fmt.Errorf("failed to update file: %w", err)
	}
	log.Printf("the records has been updated succesfully for %s", tableName)

	// update the data in-memory
	for index, updateRow := range updateRows {
		db.data[tableName][index] = updateRow
	}

	return updCnt, nil
}

func updateValues(schema Schema, exprs []SetExpression, row []interface{}) []interface{} {
	newRow := make([]interface{}, len(row))
	copy(newRow, row)
	for _, expr := range exprs {
		newRow[schema.Columns[expr.Column].Position] = expr.Value
	}

	return newRow
}

// Delete deletes data from the database.
func (db *Database) Delete(query DeleteQuery) (int, error) {
	tableName := strings.ToLower(query.TableName)
	schema, exists := db.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}

	err := validateWhereExpr(schema, query.Where)
	if err != nil {
		return 0, fmt.Errorf("invalid WHERE part: %w", err)
	}

	tableData := db.data[tableName]
	deleteCnt := 0
	deleteRows := make(map[int]struct{})
	for index, row := range tableData {
		if matches(schema, row, query.Where) {
			deleteRows[index] = struct{}{}
			deleteCnt++
		}
	}

	err = db.deleteRowsInFile(tableName, deleteRows)
	if err != nil {
		return 0, fmt.Errorf("failed to update file: %w", err)
	}
	log.Printf("the records has been deleted succesfully for %s", tableName)

	// update the data in-memory
	newRows := make([][]interface{}, 0)
	for index, row := range db.data[tableName] {
		if _, del := deleteRows[index]; del {
			continue
		}
		newRows = append(newRows, row)
	}
	db.data[tableName] = newRows

	return deleteCnt, nil
}

func tableFilePath(dbDir string, tableName string) string {
	return path.Join(dbDir, tableName) + tableFileExtension
}

func validateExpr(schema Schema, exprs []SetExpression) error {
	updateCol := make(map[string]struct{})
	for i, expr := range exprs {
		col := strings.ToLower(expr.Column)
		if _, ok := updateCol[col]; ok {
			return fmt.Errorf("column %s is mentioned twice", col)
		}

		err := validateSetExpr(schema, col, expr.Value)
		if err != nil {
			return fmt.Errorf("invalid expression at %d: %w", i, err)
		}

		updateCol[col] = struct{}{}
	}

	return nil
}

func validateSetExpr(schema Schema, column string, value interface{}) error {
	colDef, exists := schema.Columns[column]
	if !exists {
		return fmt.Errorf("column %s does not exist", column)
	}

	vt := valueType(value)
	ct := colDef.ReflectType()
	if ct != vt {
		return fmt.Errorf("types do not match: column type = %s, value type = %s", ct, vt)
	}

	return nil
}

func valueType(value interface{}) reflect.Type {
	if f, ok := value.(float64); ok && math.Trunc(f) == f {
		return reflect.TypeOf(0)
	}

	return reflect.TypeOf(value)
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

func (db *Database) deleteRowsInFile(tableName string, deleteRows map[int]struct{}) error {
	return db.updateFile(tableName, func(rows [][]interface{}) ([][]interface{}, error) {
		newRows := make([][]interface{}, 0)
		for index, row := range rows {
			if _, del := deleteRows[index]; del {
				continue
			}

			newRows = append(newRows, row)
		}

		return newRows, nil
	})
}

func (db *Database) updateRowsInFile(tableName string, updateRows map[int][]interface{}) error {
	return db.updateFile(tableName, func(rows [][]interface{}) ([][]interface{}, error) {
		for index, newRow := range updateRows {
			rows[index] = newRow
		}

		return rows, nil
	})
}

func (db *Database) writeToFileNewRows(tableName string, newRows [][]interface{}) error {
	return db.updateFile(tableName, func(rows [][]interface{}) ([][]interface{}, error) {
		return append(rows, newRows...), nil
	})
}

func (db *Database) updateFile(tableName string, updateRows func([][]interface{}) ([][]interface{}, error)) error {
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

	newRows, err := updateRows(rows)
	if err != nil {
		return fmt.Errorf("failed to update rows: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")

	err = encoder.Encode(newRows)
	if err != nil {
		return fmt.Errorf("failed to encode JSON and write to file for %s: %w", tableFilePath, err)
	}

	return nil
}

func checkFileClose(filePath string, err error) {
	if err != nil {
		panic(fmt.Errorf("failed to close file %s: %w", filePath, err))
	}
}
