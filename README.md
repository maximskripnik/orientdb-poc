OrientDB sync/async query execution POC
=======================

Usage
---

### Environments

Configuration file at `src/main//resources/application.conf` should be self-explanatory and also will show you the defaults.

But just in case:
  
`export DB_HOST=<OrientDB remote server host>`

`export DB_PORT=<OrientDB remote server port>`

`export DB_NAME=<Name of database to be used>` Make sure database is clean. It will be created and deleted after tests if it doesn't exists on server during the execution.

`export DB_ROOT_USERNAME=<OrientDB user name>` User should have rights to create/delete databases on server

`export DB_ROOT_PASSWORD=<OrientDB user password>`

`export DB_MAX_POOL_SIZE=<Maximum size of pool to be used in pool-connection tests>`

### Starting the benchmark
`sbt "run <N>"`

where N is the number of vertices you want to load orientDB with in each of the tests