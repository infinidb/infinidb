# This is InfiniDB 4.5.1

## Build prerequisites
To build InfiniDB from source you will need:

  * a CentOS/RHEL 5/6, debian 5/6 or Ubuntu 10/12 linux host configured for software development

Along with a working C++ compiler and GNU software development tools you will need the following extra packages:

  * expect
  * zlib-devel
  * ncurses-devel
  * libxml2-devel
  * readline-devel

## Build steps

### Build environment

    mkdir infinidb-src
    cd infinidb-src

### InfiniDB MySQL

    wget -Omysql-4.5.1-3.tar.gz https://github.com/infinidb/mysql/archive/4.5.1-3.tar.gz
    tar -zxf mysql-4.5.1-3.tar.gz
    ln -s mysql-4.5.1-3 mysql
    cd mysql
    ./configure --prefix=$HOME/infinidb/mysql
    make
    make install
    
### InfiniDB

    cd ..
    wget -Oinfinidb-4.5.1-3.tar.gz https://github.com/infinidb/infinidb/archive/4.5.1-3.tar.gz
    tar -zxf infinidb-4.5.1-3.tar.gz
    ln -s infinidb-4.5.1-3 infinidb
    cd infinidb
    ./configure --prefix=$HOME/infinidb
    make
    make install
    
This will leave you with `$HOME/infinidb` as a binary tree. Follow the Binary Download
instructions in the InfiniDB 4.5 Installation Guide (available on http://www.infinidb.co/).

