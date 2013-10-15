/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <conio.h>
#include <malloc.h>
#include <unistd.h>
#include <io.h>
#include <direct.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <psapi.h>

#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/algorithm/string/predicate.hpp>
namespace ba=boost::algorithm;

#include "idbregistry.h"

namespace
{

bool vFlg;
bool useSCMAPI;

SERVICE_STATUS_HANDLE svcStatusHandle;
SERVICE_STATUS svcStatus;
HANDLE svcStopEvent;

struct ProcInfo
{
	ProcInfo()
	{
	}
	ProcInfo(const string& name) :
		pName(name)
	{
	}
	string pName;
    string pCmdLine;
};

typedef vector<ProcInfo> ProcInfoVec;

const int interProcessSleepTime = 2;
const size_t cmdLineLen = 128;

string installDir;

bool shuttingDown = false;

int runIt(const string& pName)
{
	int rc = 0;
	char* cmdLine = (char*)alloca(cmdLineLen);
	strncpy_s(cmdLine, cmdLineLen, pName.c_str(), pName.size());
	PROCESS_INFORMATION pInfo;
	ZeroMemory(&pInfo, sizeof(pInfo));
	STARTUPINFO sInfo;
	ZeroMemory(&sInfo, sizeof(sInfo));

	try
	{
		if (CreateProcess(0, cmdLine, 0, 0, false, 0, 0, 0, &sInfo, &pInfo) == 0)
			return -1;
	}
	catch (exception& e)
	{
        cout << e.what() << endl;
	}
	if (WaitForSingleObject(pInfo.hProcess, INFINITE) != WAIT_OBJECT_0)
	{
		rc = -1;
		goto out;
	}

	DWORD exitCode;
	if (GetExitCodeProcess(pInfo.hProcess, &exitCode) == 0)
	{
		rc = -1;
		goto out;
	}

	if (exitCode != 0)
		rc = -1;

out:
	CloseHandle(pInfo.hProcess);
	return rc;
}

int loadBRM()
{
	// if no save file just return
	string saveFile = installDir + "\\dbrm\\BRM_saves_current";

	if (GetFileAttributes(saveFile.c_str()) == INVALID_FILE_ATTRIBUTES)
		return 0;

	// read contents of save file
	ifstream ifs(saveFile.c_str());
	if (!ifs)
		return -1;

	string saveFilePfx;
	getline(ifs, saveFilePfx);

	if (saveFilePfx.empty())
		return 0;

	// run load_brm and wait for it to finish
	string brmCmd = "load_brm \"" + saveFilePfx +"\"";
	if (runIt(brmCmd))
		return -1;
	return 0;
}

void ReportSvcStatus(DWORD currentState, DWORD win32ExitCode, DWORD waitHint)
{
	static DWORD checkPoint = 1;

	svcStatus.dwCurrentState = currentState;
	svcStatus.dwWin32ExitCode = win32ExitCode;
	svcStatus.dwWaitHint = waitHint;

	if (currentState == SERVICE_START_PENDING)
		svcStatus.dwControlsAccepted = 0;
	else
		svcStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP|SERVICE_ACCEPT_SHUTDOWN;

	if (currentState == SERVICE_RUNNING || currentState == SERVICE_STOPPED)
		svcStatus.dwCheckPoint = 0;
	else
		svcStatus.dwCheckPoint = checkPoint++;

	SetServiceStatus(svcStatusHandle, &svcStatus);
}

extern "C" DWORD WINAPI procRunner(LPVOID);

int startUp()
{
	int rc;

	if (runIt("clearShm"))
		return -1;

	rc = loadBRM();

	if (rc)
		return rc;

	ProcInfoVec procInfo;
	procInfo.push_back(ProcInfo("workernode DBRM_Worker1 fg"));
	procInfo.push_back(ProcInfo("controllernode fg"));
	procInfo.push_back(ProcInfo("DecomSvr"));
	procInfo.push_back(ProcInfo("PrimProc"));
	procInfo.push_back(ProcInfo("WriteEngineServer"));
	procInfo.push_back(ProcInfo("ExeMgr"));
	procInfo.push_back(ProcInfo("DDLProc"));
	procInfo.push_back(ProcInfo("DMLProc"));
	string mysqldCmd = "mysqld --defaults-file=" + installDir + "\\my.ini";
	procInfo.push_back(ProcInfo(mysqldCmd));

	const ProcInfoVec::size_type numProcs = procInfo.size();

	for (unsigned pidx = 0; pidx < numProcs; pidx++)
	{
		cout << "Starting " << procInfo[pidx].pName << "...";

		HANDLE thd;
		DWORD tid;
		thd = CreateThread(0, 0, procRunner, &procInfo[pidx], 0, &tid);
		if (thd == NULL)
		{
			LPTSTR lpBuffer;
			LPTSTR* lppBuffer = &lpBuffer;
			DWORD fmRes = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER|FORMAT_MESSAGE_FROM_SYSTEM,
				0, GetLastError(), 0, (LPSTR)lppBuffer, 0, 0);

			cerr << endl << "Failed to start process runner thread for " << procInfo[pidx].pName << ": ";
			if (fmRes > 0)
				cerr << lpBuffer << endl;
			else
				cerr << "Unknown error" << endl;
			return -1;
		}

		Sleep(interProcessSleepTime * 1000);
		cout << endl;
		if (useSCMAPI)
			ReportSvcStatus(SERVICE_START_PENDING, NO_ERROR, interProcessSleepTime * (static_cast<DWORD>(numProcs) - pidx - 1) * 1000);
	}

	return 0;
}

DWORD WINAPI procRunner(LPVOID parms)
{
	ProcInfo* pip = reinterpret_cast<ProcInfo*>(parms);
	ProcInfo pi = *pip;
	char* cmdLine = (char*)alloca(cmdLineLen);
	strncpy_s(cmdLine, cmdLineLen, pi.pName.c_str(), pi.pName.size());
	BOOL cpRc;
	PROCESS_INFORMATION pInfo;
	STARTUPINFO sInfo;

	for (;;)
	{
		ZeroMemory(&sInfo, sizeof(sInfo));
		ZeroMemory(&pInfo, sizeof(pInfo));
		cpRc = CreateProcess(0, cmdLine, 0, 0, false, 0, 0, 0, &sInfo, &pInfo);
		if (cpRc == 0)
		{
			LPTSTR lpBuffer;
			LPTSTR* lppBuffer = &lpBuffer;
			DWORD fmRes = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER|FORMAT_MESSAGE_FROM_SYSTEM,
				0, GetLastError(), 0, (LPSTR)lppBuffer, 0, 0);

			cerr << endl << "Failed to start process " << pip->pName << ": ";
			if (fmRes > 0)
				cerr << lpBuffer << endl;
			else
				cerr << "Unknown error" << endl;
			return -1;
		}
		WaitForSingleObject(pInfo.hProcess, INFINITE);
		CloseHandle(pInfo.hProcess);
		if (shuttingDown)
			return 0;
		Sleep(10 * 1000);
	}

	return -1;
}

int killProcByPid(DWORD pid)
{
	int rc = -1;
	HANDLE hProc;
	hProc = OpenProcess(PROCESS_TERMINATE, false, pid);
	if (hProc != NULL)
	{
		if (TerminateProcess(hProc, 0) != 0)
			rc = 0;
		CloseHandle(hProc);
	}
	return rc;
}

DWORD maxPids = 64;
DWORD* pids = 0;

int killProcByName(const string& pname)
{
	if (!pids)
		pids = (DWORD*)malloc(maxPids * sizeof(DWORD));
	DWORD needed = 0;
	if (EnumProcesses(pids, maxPids * sizeof(DWORD), &needed) == 0)
		return -1;
	while (needed == maxPids * sizeof(DWORD))
	{
		maxPids *= 2;
		pids = (DWORD*)realloc(pids, maxPids * sizeof(DWORD));
		if (EnumProcesses(pids, maxPids * sizeof(DWORD), &needed) == 0)
			return -1;
	}
	int rc = -1;
	DWORD numPids = needed / sizeof(DWORD);
	DWORD i;
	for (i = 0; i < numPids; i++)
	{
		bool found = false;
		if (pids[i] != 0)
		{
			TCHAR szProcessName[MAX_PATH] = TEXT("<unknown>");

			// Get a handle to the process.
			HANDLE hProcess = OpenProcess( PROCESS_QUERY_INFORMATION |
										   PROCESS_VM_READ,
										   FALSE, pids[i] );

			// Get the process name.
			if (NULL != hProcess )
			{
				HMODULE hMod;
				DWORD cbNeeded;

				if ( EnumProcessModules( hProcess, &hMod, sizeof(hMod), 
					 &cbNeeded) )
				{
					GetModuleBaseName( hProcess, hMod, szProcessName, 
									   sizeof(szProcessName)/sizeof(TCHAR) );
				}
			}

			if (pname == szProcessName)
				found = true;

			CloseHandle( hProcess );
		}
		if (found)
		{
			rc = killProcByPid(pids[i]);
			break;
		}
	}
	return rc;
}

int shutDown()
{
	shuttingDown = true;

	vector<string> pList;

	pList.push_back("mysqld.exe");
	pList.push_back("DMLProc.exe");
	pList.push_back("DDLProc.exe");
	pList.push_back("ExeMgr.exe");
	pList.push_back("WriteEngineServer.exe");
	pList.push_back("PrimProc.exe");
	pList.push_back("DecomSvr.exe");
	pList.push_back("controllernode.exe");
	pList.push_back("workernode.exe");

	vector<string>::iterator iter = pList.begin();
	vector<string>::iterator end = pList.end();
	vector<string>::size_type i = pList.size();
	while (iter != end)
	{
		if (killProcByName(*iter) == 0)
			Sleep(interProcessSleepTime * 1000);
		if (useSCMAPI)
			ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, static_cast<DWORD>(--i) * interProcessSleepTime * 1000);
		++iter;
	}

	if (runIt("save_brm") == 0)
		runIt("clearShm");

	if (useSCMAPI)
		ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, 0);

	return 0;
}

void svcStop()
{
	useSCMAPI = true;
	ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, 20 * 1000);
	SetEvent(svcStopEvent);
}

extern "C" VOID WINAPI svcMain(DWORD, LPSTR*);

//The SCM has asked us to start as a service
int svcStart()
{
	SERVICE_TABLE_ENTRY st[] = 
	{
		{ "InfiniDB", svcMain },
		{ 0, 0 },
	};

	//This call returns when the service has stopped
	StartServiceCtrlDispatcher(st);

	return 0;
}

extern "C" VOID WINAPI svcCtrlHandler(DWORD);

VOID WINAPI svcMain(DWORD dwArgc, LPSTR* lpszArgv)
{
	useSCMAPI = true;

	svcStatusHandle = RegisterServiceCtrlHandler("InfiniDB", svcCtrlHandler);

	svcStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
	svcStatus.dwServiceSpecificExitCode = 0;

	ReportSvcStatus(SERVICE_START_PENDING, NO_ERROR, interProcessSleepTime * 7 * 1000);

	svcStopEvent = CreateEvent(0, 1, 0, 0);

	if (startUp() != 0)
	{
		ReportSvcStatus(SERVICE_STOPPED, 1, 0);
		return;
	}

	ReportSvcStatus(SERVICE_RUNNING, NO_ERROR, 0);

	WaitForSingleObject(svcStopEvent, INFINITE);

	shutDown();

	CloseHandle(svcStopEvent);

	ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);

}

VOID WINAPI svcCtrlHandler(DWORD dwCtrl)
{
   // Handle the requested control code. 

   switch(dwCtrl) 
   {  
      case SERVICE_CONTROL_STOP:	//Notifies a service that it should stop
      case SERVICE_CONTROL_SHUTDOWN:	//Notifies a service that the system is shutting down so the service can perform cleanup tasks
		svcStop();
		break;

      case SERVICE_CONTROL_INTERROGATE:	//Notifies a service that it should report its current status information to the service control manager
         break; 
 
      case SERVICE_CONTROL_CONTINUE:	//Notifies a paused service that it should resume
         break; 
 
      case SERVICE_CONTROL_PARAMCHANGE:	//Notifies a service that its startup parameters have changed. The service should reread its startup parameters
         break; 
 
      case SERVICE_CONTROL_PAUSE:	//Notifies a service that it should pause
         break; 
 
      default: 
         break;
   }    
}

}

int main(int argc, char** argv)
{
	opterr = 0;
	vFlg = false;
	int c;
	int rc;
	useSCMAPI = false;

	while ((c = getopt(argc, argv, "v")) != EOF)
		switch (c)
		{
		case 'v':
			vFlg = true;
			break;
		case '?':
		default:
			break;
		}

	if (!vFlg)
	{
		_close(0);
		_close(1);
		_close(2);
		int fd = -1;
		_sopen_s(&fd, "NUL", _O_TEXT | _O_RDONLY, _SH_DENYNO, _S_IREAD);
		assert(fd == 0);
		fd = -1;
		_sopen_s(&fd, "NUL", _O_TEXT | _O_APPEND | _O_WRONLY, _SH_DENYNO, _S_IWRITE);
		assert(fd == 1);
		fd = -1;
		_sopen_s(&fd, "NUL", _O_TEXT | _O_APPEND | _O_WRONLY, _SH_DENYNO, _S_IWRITE);
		assert(fd == 2);
	}

	int (*cmdFp)();

	string command;
	if (argc - optind < 1)
		command  = "svcstart";
	else
		command = argv[optind];

	if (ba::istarts_with(command, "sta"))
	{
		cmdFp = startUp;
	}
	else if (ba::istarts_with(command, "sto") || ba::istarts_with(command, "sh"))
	{
		cmdFp = shutDown;
	}
	else if (ba::istarts_with(command, "sv"))
	{
		cmdFp = svcStart;
	}
	else
	{
		cerr << "Unknown command " << command << " (try 'start' or 'shutdown')" << endl;
		return 1;
	}

	installDir = IDBreadRegistry("", true);

	string newDir = installDir + "\\bin";
	rc = _chdir(newDir.c_str());

	rc = cmdFp();

	return (rc ? 1 : 0);
}
