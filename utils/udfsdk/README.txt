How to use the InfiniDB UDF SDK

Make sure you have a working GCC C++ compiler system, version 4.1.2 or later. You 
also must have the libxml2-devel package installed.
Obtain the InfiniDB source distribution for your version of InfiniDB 
from github.com.
Ideally, the machine you are compiling on and deploying on are the same machine. If this 
is not possible, they must have the same InfiniDB software installation.

Unpack the source tar file

Go into the utils/udfsdk directory.

At this point you can use the idb_add() function template in udfinfinidb.cpp and udfmysql.cpp
files to create your own function or just try that function as is.
Make the library
Stop InfiniDB
Copy the libudf_mysql.so.1.0.0 and libudfsdk.so.1.0.0 file to /usr/local/Calpont/lib on
every InfiniDB node
Start InfiniDB
In the directory /usr/local/Calpont/mysql/lib/mysql/plugin create a symbolic link called 
libudf_msql.so to the file /usr/local/Calpont/lib/libudf_msql.so.1.0.0
In the mysql client add the function (e.g. "create function idb_add returns integer soname 
'libudf_msql.so';")
You should now be able to use the idb_add() function in the select and/or where clauses 
of SQL statements.
