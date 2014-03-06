/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/** @file */

/*
 * classes isyncstream and osyncstream provide a C++ iostream interface
 * for C stdio FILE* streams. The current implementation does not provide
 * the necessary methods to support seeking. The I/O buffering of the
 * input FILE* is used. The C++ iostream library calls syncbuf::sync()
 * for every line, so output buffering is line-by-line.
 * */

/*
#include "syncstream.h"

void copyStream(istream& iss, ostream& oss)
{
	string line;
	getline(iss, line);
	while (iss.good())
	{
		oss << line << endl;
		getline(iss, line);
	}
}

main()
{
	FILE* ifp;
	FILE* ofp;

	...

	isyncstream iss(ifp);
	osyncstream oss(ofp);

	copyStream(iss, oss);

	...
}
*/

#ifndef SYNCSTREAM_H__
#define SYNCSTREAM_H__

#include <iostream>
#include <cstdio>

namespace syncstream
{

/** A streambuf implementation for C stdio FILE* streams.
 *
 * Adapted from http://www.drdobbs.com/184401305
 */
class syncbuf : public std::streambuf
{
public:
	/** ctor */
	syncbuf(FILE* f) : std::streambuf(), fptr(f) {}

protected:
	/** Write character in the case of overflow */
	virtual int overflow(int c = EOF) {
		return (c != EOF ? fputc(c, fptr) : EOF);
	}
	/** Get character in the case of overflow */
	virtual int underflow() {
		int c = getc(fptr);
		if (c != EOF)
			ungetc(c, fptr);
		return c;
	}
	/** Get character in the case of overflow and advance get pointer */
	virtual int uflow() {
		return getc(fptr);
	}
	/** put character back in the case of backup underflow */
	virtual int pbackfail(int c = EOF) {
		return (c != EOF ? ungetc(c, fptr) : EOF);
	}
	/** Synchronize stream buffer */
	virtual int sync() {
		return fflush(fptr);
	}

private:
	FILE* fptr;
};

/** An istream adaptor for input FILE* streams */
class isyncstream : public std::istream
{
public:
	/** ctor */
	isyncstream() : istream(&buf), buf(0) {}
	/** ctor */
	isyncstream(FILE* fptr) : istream(&buf), buf(fptr) {}
	/** const streambuf accessor */
	const syncbuf* rdbuf() const {
		return &buf; 
	}

private:
	syncbuf buf;
};

/** An ostream adaptor for output FILE* streams */
class osyncstream : public std::ostream
{
public:
	/** ctor */
	osyncstream() : ostream(&buf), buf(0) {}
	/** ctor */
	osyncstream(FILE* fptr) : ostream(&buf), buf(fptr) {}
	/** const streambuf accessor */
	const syncbuf* rdbuf() const {
		return &buf; 
	}

private:
	syncbuf buf;
};

}

#endif

