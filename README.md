# Table of Contents
- [Overview](#overview)
- [Usage](#usage)
- [TODO](#todo)
- [MVCC in entangleDB](#mvcc-in-entangledb)
- [SQL Query Execution in entangleDB](#sql-query-execution-in-entangledb)
- [entangleDB Raft Consensus Engine](#entangledb-raft-consensus-engine)
- [What I am trying to build](#what-i-am-trying-to-build)
  - [Distributed Consensus Engine](#1-distributed-consensus-engine)
  - [Transaction Engine](#2-transaction-engine)
  - [Storage Engine](#3-storage-engine)
  - [Query Engine](#4-query-engine)
  - [SQL Interface and PostgreSQL Compatibility](#5-sql-interface-and-postgresql-compatibility)
- [Proposed Architecture](#proposed-architecture)
- [SQL Engine](#sql-engine)
- [Raft Engine](#raft-engine)
- [Storage Engine](#storage-engine)
- [entangleDB Peers](#entangledb-peers)
- [Example SQL Queries that you will be able to execute in entangleDB](#example-sql-queries-that-you-will-be-able-to-execute-in-entangledb)
- [Learning Resources I've been using for building the database](#learning-resources-ive-been-using-for-building-the-database)

## Overview

I'm working on creating entangleDB, a project that's all about really getting to know how databases work from the inside out. My aim is to deeply understand everything about databases, from the big picture down to the small details. It's a way for me to build a strong foundation in database.

The name "entangleDB" is special because it's in honor of a friend who loves databases just as much as I do. 

The plan is to write the database in Rust. My main goal is to create something that's not only useful for me to learn from but also helpful for others who are interested in diving deep into how databases work. I'm hoping to make it postgresSQL compatible.

## Usage
Pre-requisite is to have the Rust compiler; follow this doc to install the [Rust compiler](https://www.rust-lang.org/tools/install) 

entangledb cluster can be started on `localhost` ports `3201` to `3205`:

```
(cd husky/cloud && ./build.sh)
```

Client can be used to connect with the node on `localhost` port `9605`:

```
cargo run --release --bin entanglesql

Connected to EntangleDB node "5". Enter !help for instructions.
entangledb> SELECT * FROM dishes;
poha
breads
korma
```

## TODO
1. Make the isolation level configurable; currently, it is set to repeatable read (snapshot).
2. Implement partitions, both hash and range types.
3. Utilize generics throughout in Rust, thereby eliminating the need for std::fmt::Display + Send + Sync.
4. Consider the use of runtime assertions instead of employing Error::Internal ubiquitously.
5. Revisit the implementation of time-travel queries

## MVCC in entangleDB

![image](https://github.com/TypicalDefender/entangleDB/assets/106574498/0a923e2d-75fc-469e-9ce7-504af45c73c7)

## SQL Query Execution in entangleDB
![image](https://github.com/TypicalDefender/entangleDB/assets/106574498/a90fc90c-91e7-4ee8-a06f-887629a82401)

## entangleDB Raft Consensus Engine
![image](https://github.com/TypicalDefender/entangleDB/assets/106574498/a56f02b9-d172-4ab3-8883-230d7b1326b4)

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
<img width="890" alt="Screenshot 2023-12-02 at 1 26 15 PM" src="https://github.com/TypicalDefender/entangleDB/assets/37482550/f8d262b9-618c-435d-925b-4f992076581f">

## SQL Engine

The SQL Engine is responsible for the intake and processing of SQL queries. It consists of:

- **SQL Session**: The processing pipeline within a session includes:
  - `Parser`: Interprets SQL queries and converts them into a machine-understandable format.
  - `Planner`: Devises an execution plan based on the parsed input.
  - `Executor`: Carries out the plan, accessing and modifying the database.

Adjacent to the session is the:

- **SQL Storage Raft Backend**: This component integrates with the Raft consensus protocol to ensure distributed transactions are consistent and resilient.

## Raft Engine

The Raft Engine is crucial for maintaining a consistent state across the distributed system:

- **Raft Node**: This consensus node confirms that all database transactions are in sync across the network.
- **Raft Log**: A record of all transactions agreed upon by the Raft consensus algorithm, which is crucial for data integrity and fault tolerance.

## Storage Engine

The Storage Engine is where the actual data is stored and managed:

- **State Machine Driver**: Comprising of:
  - `State Machine Interface`: An intermediary that conveys state changes from the Raft log to the storage layer.
  - `Key Value Backend`: The primary storage layer, consisting of:
    - `Bitcask Engine`: A simple, fast on-disk storage system for key-value data.
    - `MVCC Storage`: Handles multiple versions of data for read-write concurrency control.

## entangleDB Peers

- interaction between multiple database instances or "peers".

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

## Learning Resources I've been using for building the database

For a comprehensive list of resources that have been learning what to build in a distributed database, check out the [Learning Resources](https://github.com/TypicalDefender/entangleDB/blob/main/learning_resources.md) page.




