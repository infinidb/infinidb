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

#ifndef QUERYTELECLIENT_H__
#define QUERYTELECLIENT_H__

#include <unistd.h>
#include <stdint.h>

#include <boost/uuid/uuid.hpp>

#include "telestats.h"
#include "queryteleserverparms.h"
#include "querystepparms.h"

#if defined(_MSC_VER) && defined(LIBQUERYTELE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif


namespace querytele
{

class QueryTeleProtoImpl;

class QueryTeleClient
{
public:
	QueryTeleClient() : fProtoImpl(0) { }
	EXPORT explicit QueryTeleClient(const QueryTeleServerParms&);
	EXPORT ~QueryTeleClient();

	EXPORT QueryTeleClient(const QueryTeleClient& rhs);
	EXPORT QueryTeleClient& operator=(const QueryTeleClient& rhs);

	EXPORT void postQueryTele(const QueryTeleStats&);
	EXPORT void postStepTele(const StepTeleStats&);
	EXPORT void postImportTele(const ImportTeleStats&);

	EXPORT void serverParms(const QueryTeleServerParms&);
	inline const QueryTeleServerParms& serverParms() const { return fServerParms; }

	inline void stepParms(const QueryStepParms& sp) { fStepParms = sp; }
	inline const QueryStepParms& stepParms() const { return fStepParms; }
	inline QueryStepParms& stepParms() { return fStepParms; }

	EXPORT void waitForQueues();

	EXPORT static boost::uuids::uuid genUUID();
	EXPORT static int64_t timeNowms();

protected:

private:
	QueryTeleProtoImpl* fProtoImpl;
	QueryTeleServerParms fServerParms;
	QueryStepParms fStepParms;

};

}

#undef EXPORT

#endif

