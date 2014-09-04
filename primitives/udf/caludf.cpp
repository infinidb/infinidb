// $Id: caludf.cpp 866 2009-02-23 14:57:13Z rdempsey $

#include <stdint.h>
#include <cstdlib>
#include <cmath>
using namespace std;

#include "caludf.h"

extern "C"
{

int64_t cpfunc1(int64_t in)
{
	int64_t out = in + 1;
	return out;
}

int64_t cpfunc2(int64_t in)
{
	int64_t out = 0;
	union
	{
		Date d;
		uint32_t i;
	} u;
	u.i = in;
	u.d.year++;
	out = u.i;
	return out;
}

int64_t cpfunc3(int64_t in)
{
	int64_t out = in * 2;
	return out;
}

int64_t cpfunc4(int64_t in)
{
	int64_t out = in * 3;
	return out;
}

int64_t cpfunc5(int64_t in)
{
	int64_t out = abs(in);
	return out;
}

int64_t cpfunc6(int64_t in)
{
	int64_t out = log(in);
	return out;
}

int64_t cpfunc7(int64_t in)
{
	int64_t out = exp(in);
	return out;
}

int64_t cpfunc8(int64_t in)
{
	int64_t out = -in;
	return out;
}

int64_t cpfunc9(int64_t in)
{
	int64_t out = 0;
	return out;
}

}
// vim:ts=4 sw=4:

