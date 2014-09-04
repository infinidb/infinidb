// $Id: caludf.h 866 2009-02-23 14:57:13Z rdempsey $

/** @file */

#ifndef CALUDF_H__
#define CALUDF_H__

struct Date
{
	unsigned spare  : 6;
	unsigned day    : 6;
	unsigned month  : 4;
	unsigned year   : 16;
};

struct DateTime
{
	unsigned msecond : 20;
	unsigned second  : 6;
	unsigned minute  : 6;
	unsigned hour    : 6;
	unsigned day     : 6;
	unsigned month   : 4;
	unsigned year    : 16;
};

#endif
// vim:ts=4 sw=4:

