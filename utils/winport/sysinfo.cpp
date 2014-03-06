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

/* include the various windows libraries for compile */
// Don't include half the development libraries into the bot, they're not needed
#define WIN32_LEAN_AND_MEAN
// Stop windows.h from including winsock.h since winsock.h and winsock2.h are incompatible
#define _WINSOCKAPI_
#include <windows.h>
#include <TlHelp32.h>
#include "sysinfo.h"

// Since windows doesn't seem to have a simple "Number of current processes" available
// we'll just have to make our own >:(
inline int GetProcessList()
{
	HANDLE hProcessSnap;
	PROCESSENTRY32 pe32;
	long Processes = 0;


	// Snapshot the current processes and make sure it's valid
	hProcessSnap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);


	if(hProcessSnap == INVALID_HANDLE_VALUE)
		return Processes;


	// Set the size of the structure before using it.
	pe32.dwSize = sizeof(PROCESSENTRY32);


	// Retrieve information about the first process,
	// and exit if unsuccessful
	if(!Process32First(hProcessSnap, &pe32))
	{
		CloseHandle(hProcessSnap);          // clean the snapshot object
		return Processes;
	}


	// Count the total number of processes
	do
	{
		Processes++;
	}
	while(Process32Next(hProcessSnap, &pe32));


	CloseHandle(hProcessSnap);


	return Processes;
}

int sysinfo(struct sysinfo *info)
{
	SYSTEM_INFO si;
	MEMORYSTATUSEX statex;


	ZeroMemory(&si, sizeof(SYSTEM_INFO));
	statex.dwLength = sizeof(statex);
	GetSystemInfo(&si);


	if(!GlobalMemoryStatusEx(&statex))
		return -1;


	// System Uptime
	info->uptime = GetTickCount64() / 1000 % 60;


	// Load times - windows does not have this so say -1 or 0 or nothing basically
	info->loads[0] = -1;
	info->loads[1] = -1;
	info->loads[2] = -1;


	// Ram usages - note that these may not be exact to what linux has
	info->freeram = statex.ullAvailPhys;
	info->freeswap = statex.ullAvailVirtual;
	info->sharedram = 0;
	info->totalram = statex.ullTotalPhys;
	info->bufferram = statex.ullTotalPageFile;
	info->totalswap = statex.ullTotalVirtual;


	// Processes
	info->procs = GetProcessList();
	return 0;
}
