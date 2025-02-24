# GORM DuckDB Driver

This is a GORM driver for DuckDB, based on the PostgreSQL driver implementation. It uses [go-duckdb](https://github.com/marcboeker/go-duckdb) as the underlying DuckDB driver.

## Quick Start

```go
import (
  "github.com/kuwa72/duckdb"
  "gorm.io/gorm"
)

// Connect to DuckDB
db, err := gorm.Open(duckdb.Open("test.db"), &gorm.Config{})
```

## Features

- Supports basic CRUD operations
- Auto-incrementing primary keys using sequences
- Compatible with GORM's standard features

## Example

```go
package main

import (
  "fmt"
  "github.com/kuwa72/duckdb"
  "gorm.io/gorm"
)

type Person struct {
  ID   uint
  Name string
  Age  int
}

func main() {
  // Connect to DuckDB
  db, err := gorm.Open(duckdb.Open("test.db"), &gorm.Config{})
  if err != nil {
    panic("failed to connect database")
  }

  // Auto Migrate
  db.AutoMigrate(&Person{})

  // Create
  db.Create(&Person{Name: "John", Age: 30})

  // Read
  var person Person
  db.First(&person)
  fmt.Printf("Person: ID=%d, Name=%s, Age=%d\n", person.ID, person.Name, person.Age)
}
```

## Current Status

This driver is currently under development. The following features are implemented:

- [x] Basic connection and configuration
- [x] Auto-incrementing IDs using sequences
- [x] Table creation and migration
- [x] Basic CRUD operations
- [ ] Advanced query features
- [ ] Complex data types
- [ ] Transactions
- [ ] Batch operations

## Requirements

- Go 1.20 or higher
- DuckDB
- GORM v1.25.0 or higher

## Installation

```bash
go get github.com/kuwa72/duckdb
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
