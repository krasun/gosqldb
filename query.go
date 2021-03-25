package main

// CreateTableQuery represents a DDL (Data Definition Language) query to create
// new table.
type CreateTableQuery struct {
	TableName string
	Columns   []struct {
		Name string
		Type string
	}
}

// RenameTableQuery a DDL query for renaming a table.
type RenameTableQuery struct {
	TableName    string
	NewTableName string
}

// DropTableQuery a DDL query for dropping a table.
type DropTableQuery struct {
	TableName string
}

// RenameColumnQuery a DDL ("alter table") query to add a new column.
type AddColumnQuery struct {
	TableName     string
	NewColumnName string
	NewColumnType string
}

// RenameColumnQuery a DDL ("alter table") query to rename a column.
type RenameColumnQuery struct {
	TableName     string
	ColumnName    string
	NewColumnName string
}

// ChangeColumnTypeQuery a DDL ("alter table") query to change a column type.
type ChangeColumnTypeQuery struct {
	TableName     string
	ColumnName    string
	NewColumnType string
}

// DropColumnQuery a DDL ("alter table")  query to drop a column.
type DropColumnQuery struct {
	TableName  string
	ColumnName string
}

// SelectQuery is a DQL (Data Query Language) query for fetching data from the database.
type SelectQuery struct {
	ColumnNames []string
	From        string
	Where       []WhereExpression
	OrderBy     OrderByExpression
	Limit       int
}

// OrderByExpression represents ORDER BY part of the SQL query.
type OrderByExpression struct {
	ColumnName string
	Order      string
}

// WhereExpression represents WHERE part expressions of the SQL query.
type WhereExpression struct {
	LeftOperand  string
	Operation    string
	RightOperand string
}

// InsertQuery is a DML (Data Manipulation Language) query for inserting data into the database.
type InsertQuery struct {
	TableName string
	Columns   string
	Values    [][]string
}

// UpdateQuery is a DML (Data Manipulation Language) query for updating data in the database.
type UpdateQuery struct {
	TableName string
	Where     []WhereExpression
	Set       []SetExpression
	Limit     int
}

// SetExpression represents the SET part in the UPDATE SQL query.
type SetExpression struct {
	ColumnName string
	Value      string
}

// DeleteQuery is a DML (Data Manipulation Language) query for deleting data from the database.
type DeleteQuery struct {
	TableName string
	Where     []WhereExpression
	Limit     int
}