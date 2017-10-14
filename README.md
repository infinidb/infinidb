# This is InfiniDB 4.6.2

## Build prerequisites
To build InfiniDB from source you will need:

  * a CentOS/RHEL 5/6, debian 6/7 or Ubuntu 12/14 linux host configured for software development

Along with a working C++ compiler and GNU software development tools you will need the following extra packages:

  * expect
  * zlib-devel
  * ncurses-devel
  * libxml2-devel
  * readline-devel
  * flex
  * bison
  

## Build steps

### Build environment

    mkdir infinidb-src
    cd infinidb-src

### InfiniDB MySQL

    wget -Omysql-4.6.2-1.tar.gz https://github.com/infinidb/mysql/archive/4.6.2-1.tar.gz
    tar -zxf mysql-4.6.2-1.tar.gz
    ln -s mysql-4.6.2-1 mysql
    cd mysql
    ./configure --prefix=$HOME/infinidb/mysql
    make
    make install
    
### InfiniDB

    cd ..
    wget -Oinfinidb-4.6.2-1.tar.gz https://github.com/infinidb/infinidb/archive/4.6.2-1.tar.gz
    tar -zxf infinidb-4.6.2-1.tar.gz
    ln -s infinidb-4.6.2-1 infinidb
    cd infinidb
    ./configure --prefix=$HOME/infinidb
    make
    make install
    
This will leave you with `$HOME/infinidb` as a binary tree. Follow the Binary Download
instructions in the InfiniDB 4.6 Installation Guide 
(available on https://mariadb.com/services/infinidb-services).
Also an InfiniDB Enterprise version is provided and supported by MariaDB. You can also 
get additional information here: https://mariadb.com/services/infinidb-services

## How to Contribute
  * You may submit your contributions via GitHub pull requests.
  * The submission must be by the original author.
  * Along with any pull requests, please state that the contribution is your original work
and that you license the work to the project under the project's open source license
and the InfiniDB Contributor Agreement
(see InfiniDBContributorAgreement.pdf). Whether or not you state this explicitly,
by submitting any copyrighted material via pull request, email, or other means you agree to
license the material under the project's open source license and warrant that you have the
legal authority to do so.
  * The InfiniDB Project committee will review your pull request and shall decide when and
whether to merge your request in the main InfiniDB project. The InfiniDB Project
committee will inform you of any decision regarding your request.

