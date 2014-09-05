# This is InfiniDB 3.6

## Build prerequisites
To build InfiniDB from source you will need:

  * a CentOS/RHEL 5/6, debian 6/7 or Ubuntu 10/12 linux host configured for software development

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

### InfiniDB

    wget -Oinfinidb-3.6.tar.gz https://github.com/infinidb/infinidb/archive/3.6.tar.gz
    tar -zxf infinidb-3.6.tar.gz
    ln -s infinidb-3.6 infinidb
    cd infinidb
    ./configure --prefix=$HOME/infinidb
    make
    make install
    
This will leave you with `$HOME/infinidb` as a binary tree. Follow the Binary Download
instructions in the InfiniDB 3.6 Installation Guide (available on http://www.infinidb.co/).

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

