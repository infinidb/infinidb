This is InfiniDB 4.0.

To build InfiniDB from source you will need:
   o a CentOS/RHEL 5/6, debian 5/6 or Ubuntu 10/12 linux host configured for
     software development or a Windows XP-or-later host with MS VS 2008
     installed and patched to at least SP1.

Download the InfiniDB version of MySQL 5.1.39 from github.

Download this repository.

From your $HOME directory, unzip both archives.

Rename mysql-master to mysql and infinidb-master to infinidb.

cd infinidb
make
make install

