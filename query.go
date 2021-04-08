package main

// CreateTableQuery represents a DDL (Data Definition Language) query to create
// new table.
type CreateTableQuery struct {
	TableName string
	Columns   []struct {
		Name       string
		Type       string
	}
}

// SelectQuery is a DQL (Data Query Language) query for fetching data from the database.
type SelectQuery struct {
	From    string
	Where   []WhereExpression	
}

// Operand is an operand in WHERE expression
type Operand struct {
	Value interface{}
	Type  string
}

// WhereExpression represents WHERE part expressions of the SQL query.
type WhereExpression struct {
	Left      Operand
	Operation string
	Right     Operand
}

// InsertQuery is a DML (Data Manipulation Language) query for inserting data into the database.
type InsertQuery struct {
	TableName string
	Columns   []string
	Values    [][]interface{}
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
