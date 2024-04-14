![BeeDB](logo.png)

Welcome to **BeeDB**, the educational DBMS tailored for the course **Architecture and Implementation of DBMS**. 
BeeDB bridges theory and practice, fostering a deep understanding of DBMS structures and operations. 
Dive into the world of databases with BeeDB, your gateway to mastering database management.

**Architecture and Implementation of DBMS** is usually taught every summer term, see [dbis page of TU Dortmund](http://dbis.cs.tu-dortmund.de/cms/en/teaching/index.html) for more information.

## Dependencies
* `git`
* `cmake` (at least version `3.9`)
* `build-essential`
* `bison` and `flex` (and in Ubuntu the package `libfl-dev`)

## How to build
### Option (a): Build into current folder:
  * `cmake .`
  * `make` (or `make -j` for parallel compilation)

### Option (b): Build into separate `build` folder:
  * `mkdir build && cd build`
  * `cmake ..`
  * `make` (or `make -j` for parallel compilation)

### Switch between `Release` and `Debug` modes
* Default build is in `Debug` mode.
* If you want to build in `Release` mode use:
  * `cmake . -DCMAKE_BUILD_TYPE=Release`
  * **or** set `CMAKE_BUILD_TYPE` in `CMakeLists.txt`.
  
## How to use
BeeDB uses a client/server model where the executable `beedb` starts the server and `beedb_client` runs a client.

### Server
	Usage: beedb [options] db-file 

	Positional arguments:
	db-file                      	File the database is stored in. Default: bee.db

	Optional arguments:
	-h --help                    	show this help message and exit
	-p --port                    	Port of the server
	-l --load                    	Load SQL file into database.
	-q --query                   	Execute Query.
	-cmd --custom_command        	Execute custom command and exit right after.
	-k --keep                    	Keep server running after executing query, command or loading a file.
	-c --client                  	Start an additional client next to the server
	--buffer-manager-frames      	Number of frames within the frame buffer.
	--scan-page-limit            	Number of pages the SCAN operator can pin at a time.
	--enable-index-scan          	Enable index scan and use whenever possible.
	--enable-hash-join           	Enable hash join and use whenever possible.
	--enable-predicate-push-down 	Enable predicate push down and use whenever possible.
	--stats                      	Print all execution statistics

    
### Client
	Usage: beedb_client [options] host 

	Positional arguments:
	host      	Name or IP of the beedb server

	Optional arguments:
	-h --help 	show this help message and exit
	-p --port 	Port of the server
    
### Please note!
Just stopping the server by killing (or `Ctrl-C`) crashes the server; you may lose (unflushed) data.
To stop the server clean, use `:stop` command by a client.

## Configuration
Some configuration outside the console arguments is stored in the file `beedb.ini`.
* The number of pages stored as frames in the buffer manager (`buffer manager.frames`)
* The replacement strategy of frames in the buffer manager (`buffer manager.strategy`)
* The `k` parameter for `LRU-K` replacement strategy (`buffer manager.k`)
* The number of how many pages can be pinned by a scan at a time (`scan.page-limit`)
* Enable or disable usage of index scan (`optimizer.enable-index-scan`)
* Enable or disable usage of hash join (`optimizer.enable-hash-join`)
* Enable or disable predicate push down (`optimizer.enable-predicate-push-down`)

## Non-SQL Commands
Despite SQL commands, you can use the following special commands from the client.
* `:explain <query>`: prints the query plan, either as a table or a graph (a list of nodes and edges)
* `:get <option-name>`: prints either all or the secified option of the database configuration 
* `:set <option-name> <numerical-value>`: changes the specified option. Only numerical values are valid
* `:show [tables,indices,columns]`: A quick way to show available tables, their columns or indices
* `:stop`: Stops the server (and flushes all data to the disk).

## Examples

##### Import and SQL file (containing `CREATE` and `INSERT`)
`./beedb -l movies.sql`

##### Run a single query and terminate
`./beedb -q "SELECT * FROM movie;"`

##### Run a query and open console afterwards
`./beedb -q "SELECT * FROM movie;" -c`

##### Start the BeeDB server only (connect clients later)
`./beedb`

##### Start the BeeDB server and open a client console
`./beedb -c`

#### Start a BeeDB client (you can start multiple ones)
`./beedb_client`

# For developers
* If you want to commit to the repository please `make git-hook` before commit.
  
# Credits
* Thanks to **p-ranav** for `argparse` (MIT license, [See on GitHub](https://github.com/p-ranav/argparse)).
* Thanks to **antirez** for `linenoise` (BSD license, [See on Github](https://github.com/antirez/linenoise)).
* Thanks to **nlohmann** and further contributors for `nlohmann_json` (MIT license, [See on GitHub](https://github.com/nlohmann/json)).