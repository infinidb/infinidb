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

#include <boost/thread/mutex.hpp>

#define LOG_BUF_SIZE 256
#define TIMESTAMP_SIZE 16
#define TIME_BUF_SIZE 36

class WinSyslog
{
public:
   /**
    * @brief dtor
    *        Might work as private, but not sure.
    */
    ~WinSyslog();
   /**
    * @brief Singleton accessor.
    *        Doesn't need locking because we use early construction.
    */
    static WinSyslog* instance()
	{
		if (!fpSysLog)
			fpSysLog = new WinSyslog();
		return fpSysLog;
	};

    int     OpenLog() {return 0;};
	int     CloseLog() {return 0;};

   /**
    * @brief Logging function.
    *        Called by ::syslog to do the logging
    */
	int     Log(int priority, const char* format, va_list& args);

private:
    // Disable public construction, destruction and assignment
	WinSyslog();
	WinSyslog(WinSyslog&);
    void operator=(WinSyslog const&); 

   /**
    * @brief Archiving function.
    *        Called by Log() at midnight to archive the log 
    *        file and create a new one
    */
    void    Archive(const tm& nowtm);

	time_t  fLastArchiveTime;
	int     fLastArchiveDay;
	string  fLogDirName;
	string  fLogFileName;
	string  fTimeFileName;
	char    fLogLineHeader[LOG_BUF_SIZE];
	int	    fLogLineheaderSize;
  
    // Singleton pointer
    static  WinSyslog* fpSysLog;
    // Synchronize objects for Archiving.
	static  boost::mutex fMutex;
    static  HANDLE fhMutex;
    static  bool fbGoodIPMutex;
};