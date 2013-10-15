#ifndef QFE_SOCKTYPE_H__
#define QFE_SOCKTYPE_H__

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#endif

#ifdef _MSC_VER
typedef SOCKET SockType;
#define SockReadFcn qfe::socketio::reads
#define SockWriteFcn qfe::socketio::writes
#else
typedef int SockType;
#define SockReadFcn qfe::socketio::readn
#define SockWriteFcn qfe::socketio::writen
#endif

#endif

