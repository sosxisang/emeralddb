#!/bin/sh
rm configure.in
autoscan
mv configure.scan configure.in
cp configure.in.bak configure.in
aclocal
autoconf
autoheader
automake --add-missing
./configure CXXFLAGS= CFLAGS=
make
