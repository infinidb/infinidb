// $Id: consts.h 27 2006-10-20 13:03:31Z rdempsey $

#ifndef CONSTS_H__
#define CONSTS_H__

#include "bytestream.h"
const uint64_t defaultHJPmMaxMemorySmallSide = 67108864LL;
const messageqcpp::ByteStream::octbyte PrimSize = 2048;
// const messageqcpp::ByteStream::octbyte PrimSize = 1024;
const int g_max_tpdu = 1500;
int g_sqns;
int g_rcvbuf;
int g_sndbuf;

#endif

