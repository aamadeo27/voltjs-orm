# voltjs-orm
VoltDB ORM for Nodejs

Voltjs-orm is a library that provides ORM (Object/Relational Mapping) support for VoltDB in nodejs applications.
It features limited transactional operations outside of stored procedures, generation of storedprocedures that can handle basic crud operations for every model defined.

## v1 Alpha Release
```bash
$ npm install --save voltjs-orm

# To generate stored procedures is needed 
# the voltdb-version.jar that is in VOLTDB_HOME/voltdb/voltdb-version.jar
export VOLTDB_HOME=<voltdbhome>
```


## Table of Contents
- [Installation](#installation)
- [Features](#features)
- [Documentation](#documentation)
- [Resources](#resources)


Sequelize follows [SEMVER](http://semver.org). Supports Node v? and above to use ES7 features.

## Features

* Generates the following stored procedures associated to a namespace.

   * SPLoader SinglePartition Loader

   * SPLLoader SinglePartition Locking Loader

   * SPWriter SinglePartition UPDATE/DELETE/INSERT Executor

   * MPLoader MultiPartition Loader

   * MPLLoader MultiPartition Locking Loader

   * MPWriter MultiPartition UPDATE/DELETE/INSERT Executor

* Through models. Specifying:

   * Columns (in order)
   
   * Primary Key
   
   * Partition Key

* Promises
* Limited Transactions
   * The operations select for update, delete, update, insert that are issued using the library are transactional among them. The operations of insert, delete, update and upsert issed outside the library will ignore the locks issued by the library.
   * Select for update supported.
   * The operations suported to write are:
      * insert
      * delete where pk-condition
      * update where pk-condition
  
   \* The select for update, update and delete operations are only present if the model has a primary key defined.
