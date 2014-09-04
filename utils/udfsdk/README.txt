How to use the InfiniDB UDF SDK

Make sure you have a working GCC C++ compiler system, version 4.1.2 or later. You 
also must have the libxml2-devel package installed.
Obtain the InfiniDB Community Edition source distribution for your version of InfiniDB 
from www.infinidb.org.
Obtain the InfiniDB UDF SDK from Calpont for your version of InfiniDB.
Ideally, the machine you are compiling on and deploying on are the same machine. If this 
is not possible, they must have the same InfiniDB software installation.

All of the following steps assume you will be working out of a directory called idbudfsdk 
in your home directory.

Unpack the source tar file
Unpack the UDF SDK
In the udfsdk directory edit the Makefile and make changes to match your development 
environment as directed in the file.

At this point you can use the idb_add() function template in udfsdk.cpp to create your 
own function or just try that function as is.
Make the library
Stop InfiniDB
Copy the libudfsdk.so.1.0.0 file to /usr/local/Calpont/lib on every InfiniDB node
Start InfiniDB
In the directory /usr/local/Calpont/mysql/lib/mysql/plugin create a symbolic link called 
libudfsdk.so to the file /usr/local/Calpont/lib/libudfsdk.so.1.0.0
In the mysql client add the function (e.g. "create function idb_add returns integer soname 
'libudfsdk.so';")
You should now be able to use the idb_add() function in the select and/or where clauses 
of SQL statements.
