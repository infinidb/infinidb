This is InfiniDB 4.0.
=====================

To build InfiniDB from source you will need:

  * a CentOS/RHEL 5/6, debian 5/6 or Ubuntu 10/12 linux host configured for software development

Along with a working C++ compiler and GNU software development tools you will need the following extra packages:

  * expect
  * zlib-devel
  * ncurses-devel
  * libxml2-devel
  * readline-devel

Build the easy way:

    git clone http://github.com/infinidb/infinidb
    cd infinidb
    ./build/src-build --prefix=$HOME

Build the hard way:

    cd $HOME
    rm -rf mysql
    git clone http://github.com/infinidb/mysql
    cd mysql
    ./configure --prefix=$HOME/Calpont/mysql
    make
    make install
    cd $HOME
    rm -rf infinidb
    git clone http://github.com/infinidb/infinidb
    cd infinidb
    ./configure --prefix=$HOME
    make
    make install
    
Either way you will end up with `$HOME/Calpont` as a binary tree. Follow the Binary Download
instructions in the InfiniDB 4.0 Installation Guide (available on http://www.calpont.com/).
