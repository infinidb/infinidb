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

#include "idbregistry.h"

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <string>
using namespace std;

const string IDBreadRegistry(const string& name, bool returnShortName)
{
	HKEY hkResult;
	LONG lResult;
	lResult = RegOpenKeyEx(HKEY_LOCAL_MACHINE, "Software\\Calpont\\InfiniDB", 0, KEY_READ, &hkResult);
	if (lResult != ERROR_SUCCESS)
		return string();
	DWORD cbData = 1024;
	TCHAR CfnameBuf[1024];
	lResult = RegQueryValueEx(hkResult, name.c_str(), 0, 0, (LPBYTE)CfnameBuf, &cbData);
	CloseHandle(hkResult);
	if (lResult != ERROR_SUCCESS)
		return string();
	if (!returnShortName)
		return string(CfnameBuf);

	cbData = 1024;
	TCHAR snbuffer[1024];
	lResult = GetShortPathName(CfnameBuf, snbuffer, cbData);
	if (lResult == 0 || lResult > 1024)
		return string();
	return string(snbuffer);
}