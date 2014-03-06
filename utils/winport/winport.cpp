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

#include <time.h>
#include <string>
#include <sys/types.h>
#include <map>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
namespace fs = boost::filesystem;

using namespace std;

#include "unistd.h"
#include "stdint.h"
#include "sys/time.h"
#include "syslog.h"
#include "idbregistry.h"
#include "WinSyslog.h"
#include "io.h"
//This is the number of msecs between 1601 and 1970 (+/-)
//#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
//This is the number of 100-nsec intvls btwn 1601 and 1970 (+/-)
const unsigned __int64 EPOCH_DELTA = ((1970Ui64 - 1601Ui64) * 365Ui64 +
									  ((1970Ui64 - 1601Ui64) / 4Ui64) -
									  ((1970Ui64 - 1601Ui64) / 100Ui64) +
									  ((1970Ui64 - 1601Ui64) / 400Ui64)) *
									  86400Ui64 * 1000Ui64 * 1000Ui64 * 10Ui64;

// returns the secs+usecs since the epoch
int gettimeofday(struct timeval* tvp, struct timezone* tzp)
{
  FILETIME ft;
  unsigned __int64 tmpres = 0;
  static int tzflag;
 
  if (0 != tvp)
  {
    //returns the current time as the number of 100-nanosecond intervals since January 1, 1601 (UTC)
    GetSystemTimeAsFileTime(&ft);
 
    tmpres |= ft.dwHighDateTime;
    tmpres <<= 32;
    tmpres |= ft.dwLowDateTime;
 
    /*converting file time to unix epoch*/
    tmpres -= EPOCH_DELTA; 
    tmpres /= 10;  /*convert into microseconds*/
    tvp->tv_sec = (long)(tmpres / 1000000UL);
    tvp->tv_usec = (long)(tmpres % 1000000UL);
  }
 
  if (0 != tzp)
  {
    if (!tzflag)
    {
      _tzset();
      tzflag++;
    }
    tzp->tz_minuteswest = _timezone / 60;
    tzp->tz_dsttime = _daylight;
  }
 
  return 0;
}

int closelog(...)
{
	return 0;
}

int openlog(...)
{
	return 0;
}

int syslog(int priority, const char* format, ...)
{
    int rtn;
    va_list args;
    va_start(args, format);
    rtn = WinSyslog::instance()->Log(priority, format, args);
    va_end(args);
    return rtn;
}

int fcntl(int i1, int i2, ...)
{
	return 0;
}

int inet_aton(const char* c, struct in_addr* p)
{
	p->S_un.S_addr = inet_addr(c);
	return 1;
}

int flock(int i1, int i2)
{
	return 0;
}

int usleep(unsigned int usecs)
{
	unsigned int msecs;
	//cvt usecs to msecs
	msecs = usecs / 1000;
	if (msecs == 0) msecs++;
	Sleep(msecs);
	return 0;
}

int fork()
{
	return -1;
}

int getpagesize()
{
	return 4096;
}

struct tm* idb_localtime_r(const time_t* tp, struct tm* tmp)
{
	time_t t = *tp;
	errno_t p = 0;
	p = localtime_s(tmp, &t);
	if (p != 0)
		memset(tmp, 0, sizeof(struct tm));
	return tmp;
}

//FIXME: need a better impl!
long clock_gettime(clockid_t, struct timespec* tp)
{
	SYSTEMTIME st;
	GetSystemTime(&st);
	tp->tv_sec = st.wHour * 3600 + st.wMinute * 60 + st.wSecond;
	tp->tv_nsec = st.wMilliseconds * 1000000;
	return 0;
}

#if _MSC_VER < 1600
lldiv_t lldiv(const long long numer, const long long denom)
{
	lldiv_t ret;
	ret.quot = numer / denom;
	ret.rem = numer % denom;
	return ret;
}
#endif

unsigned int sleep(unsigned int secs)
{
	Sleep(secs * 1000);
	return 0;
}
int pipe(int fds[2])
{
	return -1;
}

pid_t getppid()
{
	return -1;
}

pid_t waitpid(pid_t, int*, int)
{
	return -1;
}

int kill(pid_t, int)
{
	return -1;
}

int setuid(uid_t)
{
	return -1;
}

string IDBSysErrorStr(DWORD err) 
{ 
    // Retrieve the system error message for the last-error code

	string errstr;

    LPVOID lpMsgBuf;

    FormatMessage(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | 
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        err,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPTSTR) &lpMsgBuf,
        0, NULL );

    // Display the error message and exit the process

	errstr = (LPCTSTR)lpMsgBuf;

    LocalFree(lpMsgBuf);

	return errstr;
}
