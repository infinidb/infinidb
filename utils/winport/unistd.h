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

#ifndef _UNISTD_H
#define _UNISTD_H 1
#include <stdio.h>
#include <io.h>
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <process.h>
#include <winsock2.h>
#include <inaddr.h>
#include <stdint.h>
#include <time.h>
#include "inttypes.h"
#ifdef __cplusplus
#include <string>
extern "C" {
#endif

#if _MSC_VER < 1800
extern unsigned long long strtoull(const char*, char**, int);
extern long long atoll(const char*);
#if _MSC_VER < 1600
extern lldiv_t lldiv(const long long, const long long);
#endif
#endif
extern unsigned int sleep(unsigned int);

#define strerror_r(e, b, l) strerror_s((b), (l), (e))

#ifndef F_OK
#define F_OK 00
#define W_OK 02
#define R_OK 04
#define X_OK 00
#endif
#ifndef F_RDLCK
#define F_RDLCK 1
#define F_SETLKW 2
#define F_UNLCK 3
#define F_WRLCK 4
#define F_SETLK 5
#endif
#ifndef LOCK_SH
#define LOCK_SH 0
#define LOCK_UN 1
#define LOCK_EX 2
#endif
struct flock
{
	int l_type;
	int l_whence;
	int l_start;
	int l_len;
	int l_pid;
};
extern int flock(int, int);
extern int fcntl(int, int, ...);
#ifndef _my_pthread_h
struct timespec
{
	long tv_sec;
	long tv_nsec;
};
#endif
int poll(struct pollfd*, unsigned long, int);
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
extern int inet_aton(const char*, struct in_addr*);
struct timezone 
{
  int  tz_minuteswest; /* minutes W of Greenwich */
  int  tz_dsttime;     /* type of dst correction */
};
extern int gettimeofday(struct timeval*, struct timezone*);
#define ctime_r(tp, b) ctime_s((b), sizeof(b), (tp))

//These are also in MySQL, so we need to fudge them...
#ifndef _my_pthread_h
#define localtime_r idb_localtime_r
extern struct tm* idb_localtime_r(const time_t*, struct tm*);
#define strtoll _strtoi64
#define strtoull _strtoui64
#define crc32 idb_crc32
extern unsigned int idb_crc32(const unsigned int, const unsigned char*, const size_t);
#endif

#define CLOCK_REALTIME 1
#define CLOCK_MONOTONIC 2
extern long clock_gettime(clockid_t, struct timespec*);

extern int syslog(int, const char*, ...);
#ifdef __cplusplus
extern int closelog(...);
extern int openlog(...);
#else
extern int closelog();
extern int openlog();
#endif
extern int usleep(unsigned int);
extern int fork();

extern int getpagesize();

extern int pipe(int[2]);
extern pid_t getppid();
extern pid_t waitpid(pid_t, int*, int);

#define WIFEXITED(x) 0
#define WEXITSTATUS(x) 0
#define WIFSIGNALED(x) 0
#define WTERMSIG(x) 0

#define WNOHANG 0x01
#define WUNTRACED 0x02
#define WCONTINUED 0x04

extern int kill(pid_t, int);
extern int setuid(uid_t);

#define snprintf _snprintf

#ifdef __cplusplus
}

extern int getopt(int argc, char* const* argv, const char* optstring);
extern char* optarg;
extern int optind;
extern int opterr;
extern int optopt;

extern std::string IDBSysErrorStr(DWORD err);

#endif

#endif

