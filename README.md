# spark-postgres-json-migration

High-performance PostgreSQL to PostgreSQL data migration tool using Apache Spark and Scala.

## Features
- ⚡ Batch processing (10,000 rows per batch)
- 📊 Handles JSON columns automatically
- 🔄 TRUNCATE + APPEND strategy
- 📈 Performance metrics and progress tracking
- 🛡️ Error handling and connection pooling

## Tech Stack
- Scala 2.12
- Apache Spark 3.x
- PostgreSQL JDBC Driver
- SBT

## Usage
```scala
// Configure source and target databases
val jdbcUrl = "jdbc:postgresql://localhost:5432/yourdb"
// Run the migration
sbt run
```

## Performance
Processes millions of rows efficiently with automatic JSON type casting.
