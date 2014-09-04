#!/bin/bash
# $Id$

test -f rules.tmpl || exit 1

mv -f rules.tmpl temp
sed 's/^DEBUG_FLAGS=.*$/DEBUG_FLAGS=-g0 -O3 -fno-strict-aliasing/g' <temp >rules.tmpl
rm -f temp

