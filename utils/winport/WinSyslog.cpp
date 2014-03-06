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
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
namespace fs = boost::filesystem;

using namespace std;

#include "unistd.h"
#include "stdint.h"
#include "sys/time.h"
#include "syslog.h"
#include "idbregistry.h"
#include "WinSysLog.h"

// WinSyslog class encapsulates what substitutes for the syslog in the 
// InfiniDB Windows version.

// Singleton instance
WinSyslog* WinSyslog::fpSysLog = WinSyslog::instance();
// For archive coordination.
// We have two mutexes. Only one will be used. We want to use the inter-process
// mutex name fhMutex. If creation succeeds, we set fbGoodIPMutex so the code knows
// it's good. If we fail, fMutex (Only this processes threads) will be used - it's
// better than nothing.
boost::mutex WinSyslog::fMutex;                 // Failsafe Intra process mutex
HANDLE WinSyslog::fhMutex;                      // Interprocess named mutex
bool WinSyslog::fbGoodIPMutex = false;
#define WINSYSLOG_MUTEX_NAME "WinSysLogMutex"   // For the Windows Named Mutex

// Constructor, wherein we initialize everything
WinSyslog::WinSyslog() :
    fLastArchiveTime(0),
    fLastArchiveDay(0),
    fLogLineheaderSize(0)
{
    fLogDirName = IDBreadRegistry("") + "\\log";
    fLogFileName = IDBreadRegistry("") + "\\log\\InfiniDBLog.txt";
    fTimeFileName = IDBreadRegistry("") + "\\log\\InfiniDBLastArchive.dat";

    // Set up a buffer with all the line header except the timestamp.
    // Leave room in the front for the timestamp.
    // This header will approximate the linux standard syslog log header.
    memset(fLogLineHeader, 0, LOG_BUF_SIZE);
    char* pEnd = fLogLineHeader + LOG_BUF_SIZE;
    char* pHeader = fLogLineHeader + TIMESTAMP_SIZE;
    char  exePath[LOG_BUF_SIZE];
    DWORD nameSize = (DWORD)(pEnd-pHeader);
    GetComputerName(pHeader, &nameSize);
    pHeader += nameSize;
    *pHeader++ = ' ';
    GetModuleFileName(0, exePath, LOG_BUF_SIZE);
    _splitpath_s(exePath, NULL, 0, NULL, 0, pHeader, pEnd-pHeader, NULL, 0); 
    pHeader += strlen(pHeader);;
    *pHeader++ = '[';
    int pid = GetCurrentProcessId();
    snprintf(pHeader, pEnd-pHeader, "%d", pid);
    pHeader += strlen(pHeader);;
    *pHeader++ = ']';
    *pHeader++ = ':';
    fLogLineheaderSize = (int)(pHeader - fLogLineHeader);
    // Set up the date we compare against for archiving.
    // This will either be today (if fTimeFileName doesn't exist)
    // or the the date stored in fTimeFileName.
	time_t now = time(0);
    FILE* lf;
    lf = fopen(fTimeFileName.c_str(), "r+b");
    if (lf == NULL)		// First time archiving.
    {
        fLastArchiveTime = now;
        // Persist the timestamp of last archive.
        lf = fopen(fTimeFileName.c_str(), "w+b");
        if (lf != 0)
        {
            fwrite(&fLastArchiveTime, sizeof(time_t), 1, lf);
            fclose(lf);
        }
    }
    else
    {
        fread(&fLastArchiveTime, sizeof(time_t), 1, lf);
        fclose(lf);
    }
    struct tm lasttm;
    localtime_s(&lasttm, &fLastArchiveTime);
    fLastArchiveDay = lasttm.tm_yday;

    // Create an interprocess mutex to coordinate the archiving function
    fhMutex = CreateMutex(NULL, FALSE, WINSYSLOG_MUTEX_NAME);
    if (fhMutex == NULL) 
    {
        syslog(LOG_ERR, "WinSyslog CreateMutex error: %d\n", GetLastError());
    }
    else
    {
        fbGoodIPMutex = true;
    }
}

WinSyslog::~WinSyslog()
{
    CloseHandle(fhMutex);
}

// Log() is called by ::syslog() to actually write stuff to the file
int WinSyslog::Log(int priority, const char* format, va_list& args)
{
    struct tm nowtm;
    FILE*   f;
	time_t  now = time(0);
	
    localtime_s(&nowtm, &now);

    // If now isn't the same day as the last archive date, archve.
    if (nowtm.tm_yday != fLastArchiveDay)
    {
        Archive(nowtm);
    }

    // Log the line.
    strftime(fLogLineHeader, TIMESTAMP_SIZE, "%b %d %H:%M:%S", &nowtm);
    fLogLineHeader[TIMESTAMP_SIZE-1] = ' ';
    f = fopen(fLogFileName.c_str(), "a+");
    if (f == 0) return -1;
    fwrite(fLogLineHeader, 1, fLogLineheaderSize, f);
    vfprintf(f, format, args);
    fwrite("\n", 1, 1, f);
    fclose(f);
    return 0;
}

// Archive() is called by Log() when the date changes
// Here we rename our log file to an archive name and delete
// any old files.
void WinSyslog::Archive(const tm& nowtm)
{        
    struct tm   yesterdaytm;
    bool        bArchive = true;
    time_t      writeTime;
    char        ctimebuf[TIME_BUF_SIZE] = {0};
    DWORD       dwWaitResult;
    char*       szError = "";
    // Lock it so we don't have two threads archiving. 
    if (fbGoodIPMutex)
    {
        // We have a good interprocess mutex. Try for one second to
        // lock it. Abandon trying after a second. Something's stuck.
        dwWaitResult = WaitForSingleObject(fhMutex, 1000);
 
        switch (dwWaitResult) 
        {
            // The thread got ownership of the mutex. Continue on.
            case WAIT_OBJECT_0: 
                break; 
            // The thread got ownership of an abandoned mutex, most likely because
            // some process crashed in the middle of archiving. Continue on. 
            case WAIT_ABANDONED: 
                break; 
            // We timed out. Something's not right. Don't archive.
            case WAIT_TIMEOUT:
                szError = "WinSyslog::Archive WAIT_TIMEOUT";
                bArchive = false;
                break;
            // Horrible failure. Don't archive
            case WAIT_FAILED:
                szError = "WinSyslog::Archive WAIT_FAILED";
                bArchive = false;
                break;
        }
    }
    else
    {
        boost::mutex::scoped_lock lock(fMutex);
    }

    try
    {
        // Check to see if any other thread or process already archived.
        // If the value in fTimeFileName is different than our saved value
        // Then somebody else beat us to it. Don't do it again.
        time_t  storedLastArchiveTime;
        FILE* lf;
        lf = fopen(fTimeFileName.c_str(), "r+b");
        if (lf != NULL)
        {
            fread(&storedLastArchiveTime, sizeof(time_t), 1, lf);
            fclose(lf);
            if (storedLastArchiveTime != fLastArchiveTime)
            {
                bArchive = false;
            }
        }
        else
        {
            // If we had something go wrong earlier, and then failed to read the Time file,
            // this should prevent retrying to archive every log line.
            storedLastArchiveTime = fLastArchiveTime = time(NULL);
        }
        if (bArchive == false)
        {
            // Something's not right. Try to set the internals so we don't attempt
            // archiving again.
            fLastArchiveTime = storedLastArchiveTime;
            fLastArchiveDay = nowtm.tm_yday;
            if (szError)
            {
                syslog(LOG_ERR, szError);
            }
            return;
        }
        // Get last archive date (usually yesterday)
        yesterdaytm = nowtm;
        yesterdaytm.tm_mday -= 1;					// May be 0. mktime() adjusts accordingly.
        time_t yesterday = mktime(&yesterdaytm);
        localtime_s(&yesterdaytm, &yesterday);
        string archiveFileName = fLogFileName;
        size_t tail = archiveFileName.find(".txt");
        archiveFileName.erase(tail, 4);
        strftime(ctimebuf, TIME_BUF_SIZE, "-%Y-%m-%d.txt", &yesterdaytm);
        archiveFileName += ctimebuf;
        rename(fLogFileName.c_str(), archiveFileName.c_str());
        
        // Persist the timestamp of last archive. In case of reboot, we
        // can still archive properly
        lf = fopen(fTimeFileName.c_str(), "w+b");
        if (lf != 0)
        {
            fLastArchiveTime = time(0);
            fwrite(&fLastArchiveTime, sizeof(time_t), 1, lf);
            fclose(lf);
        }
        fLastArchiveDay = nowtm.tm_yday;

        // Get the dates of all archive files.
        fs::path sourceDir(fLogDirName); 
        fs::directory_iterator iter(sourceDir);
        fs::directory_iterator end_iter;
        std::multimap<std::time_t, fs::path> fileSet;  // Stays sorted
        while (iter != end_iter)
        {
            fs::path archiveFile = *iter;
            if (fs::is_regular_file(archiveFile) )
            {
                if (archiveFile.extension() == ".txt")
                {
                    writeTime = fs::last_write_time(archiveFile);
                    fileSet.insert(make_pair(writeTime, archiveFile));
                }
            }
            ++iter;
        }
        // Delete anything past the first seven newest files
        typedef std::multimap<std::time_t, fs::path>::iterator PATH_ITER;
        PATH_ITER fileIter = fileSet.begin();
        PATH_ITER fileIterEnd = fileSet.end();
        std::reverse_iterator<PATH_ITER> revEnd(fileIter);
        std::reverse_iterator<PATH_ITER> revIter(fileIterEnd);
        int cnt = 0;
        for (; revIter != revEnd; ++revIter)
        {
            if (cnt++ < 7)
                continue;
            _wunlink(revIter->second.c_str());
        }
    }
    catch (exception& e)
    {
        syslog(LOG_ERR, "WinSyslog::Archive Exception %s", e.what());
    }
    catch (...)
    {
        syslog(LOG_ERR, "WinSyslog::Archive Exception ...");
    }
    if (fbGoodIPMutex)
    {
        if (!ReleaseMutex(fhMutex))
            syslog(LOG_ERR, "WinSyslog::Archive ReleaseMutex failed %d", GetLastError());
    }
}
