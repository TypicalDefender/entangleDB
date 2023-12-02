
# entangleDB Initial Proposal

## Overview

I'm working on creating entangleDB, a project that's all about really getting to know how databases work from the inside out. My aim is to deeply understand everything about databases, from the big picture down to the small details. It's a way for me to build a strong foundation in database.

The name "entangleDB" is special because it's in honor of a friend who loves databases just as much as I do. It's a name that reflects both our passion for databases.

Right now, I'm thinking about whether to use Rust or Go to build it. My main goal is to create something that's not only useful for me to learn from but also helpful for others who are interested in diving deep into how databases work. I'm hoping to make it postgresSQL compatible.

## What I am trying to build

### 1. Distributed Consensus Engine

The design for entangleDB centers around a custom-built consensus engine, intended for high availability in distributed settings. This engine will be crucial in maintaining consistent and reliable state management across various nodes.

A key focus will be on linearizable state machine replication, an essential feature for ensuring data consistency across all nodes, especially for applications that require strong consistency.

### 2. Transaction Engine

 The proposed transaction engine for entangleDB is committed to adhering to ACID properties, ensuring reliability and integrity in every transaction.

The plan includes the implementation of Snapshot Isolation and Serializable Isolation, with the aim of optimizing transaction handling for enhanced concurrency and data integrity.

### 3. Storage Engine

 The planned storage engine for entangleDB will explore a variety of storage formats to find and utilize the most efficient methods for data storage and retrieval.

The storage layer is being designed for flexibility, to support a range of backend technologies and meet diverse storage requirements.

### 4. Query Engine

The development of the query engine will focus on rapid and effective query processing, utilizing advanced optimization algorithms.

A distinctive feature of entangleDB will be its ability to handle time-travel queries, allowing users to access and analyze data from different historical states.

### 5. SQL Interface and PostgreSQL Compatibility

The SQL interface for entangleDB is intended to support a wide array of SQL functionalities, including complex queries, joins, aggregates, and window functions.

Compatibility with PostgreSQL’s wire protocol is a goal, to facilitate smooth integration with existing PostgreSQL setups and offer a solid alternative for database system upgrades or migrations.

## Proposed Architecture




## Example SQL Queries that you will be able to execute in entangleDB

```sql
-- Transaction example with a table creation, data insertion, and selection
BEGIN;

CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, department VARCHAR);
INSERT INTO employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'HR');
SELECT * FROM employees;

COMMIT;

-- Aggregation query with JOIN
SELECT department, AVG(salary) FROM employees JOIN salaries ON employees.id = salaries.emp_id GROUP BY department;

-- Time-travel query
SELECT * FROM employees AS OF SYSTEM TIME '-5m';
```
