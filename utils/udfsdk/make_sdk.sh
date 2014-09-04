#!/bin/bash

rm -rf udfsdk udfsdk.tar.gz
mkdir udfsdk
cp udfsdk.cpp udfsdk.h README.txt udfsdk
cp Makefile.sdk udfsdk/Makefile
tar -zcvf udfsdk.tar.gz udfsdk
rm -rf udfsdk

