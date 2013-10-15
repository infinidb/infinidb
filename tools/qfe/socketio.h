#ifndef QFE_SOCKETIO_H__
#define QFE_SOCKETIO_H__

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#else
#include <stdint.h>
#endif
#include <unistd.h>
#include <string>

#include "socktype.h"

namespace qfe
{
namespace socketio
{

#ifndef _MSC_VER
void readn(int fd, void* buf, const size_t wanted);
size_t writen(int fd, const void* data, const size_t nbytes);
#else
void reads(SOCKET fd, void* buf, const size_t wanted);
size_t writes(SOCKET fd, const void* buf, const size_t len);
#endif
uint32_t readNumber32(SockType);
std::string readString(SockType);
void writeString(SockType, const std::string&);

} //namespace qfe::socketio
} //namespace qfe

#endif

