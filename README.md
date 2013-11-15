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

Clone or download this repository.

    git clone https://github.com/infinidb/infinidb
    cd infinidb
    ./build/src-build --prefix=$HOME

