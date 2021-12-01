# gosqldb

[![Build Status](https://app.travis-ci.com/krasun/gosqldb.svg?branch=main)](https://app.travis-ci.com/krasun/gosqldb)
[![codecov](https://codecov.io/gh/krasun/gosqldb/branch/main/graph/badge.svg?token=8NU6LR4FQD)](https://codecov.io/gh/krasun/gosqldb)
[![Go Report Card](https://goreportcard.com/badge/github.com/krasun/gosqldb)](https://goreportcard.com/report/github.com/krasun/gosqldb)
[![GoDoc](https://godoc.org/https://godoc.org/github.com/krasun/gosqldb?status.svg)](https://godoc.org/github.com/krasun/gosqldb)

**WORK IN PROGRESS**

`gosqldb` is a fully functional key-value persistent database written in Go that supports different storage engines and simple SQL. 

**It is not intended for production use**. The primary purpose of the database is to learn and experiment with different storage engines and database concepts. 

Features: 
- [simple SQL](https://github.com/krasun/gosqlparser) to execute queries against the database; 
- [in-file B+ tree](https://github.com/krasun/fbptree) storage engine; 
- [LSM tree](https://github.com/krasun/lsmtree) storage engine;
- [in-memory B+ tree](https://github.com/krasun/bptree) storage engine;  
- the code is structured and optimized for learning purposes; 
- the core parts of the database are separate projects on their own. 

## Play 

... 

## License 

**gosqldb** is released under [the MIT license](LICENSE).