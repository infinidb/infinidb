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

/*******************************************************************************
* $Id: we_ddlcommandproc.h 3043 2011-08-29 22:03:03Z chao $
*
*******************************************************************************/
#ifndef WE_DDLCOMMANDPROC_H__
#define WE_DDLCOMMANDPROC_H__

#include <unistd.h>
#include "bytestream.h"
#include "we_messages.h"
#include "dbrm.h"
#include "we_message_handlers.h"
#include "liboamcpp.h"
#include <boost/date_time/gregorian/gregorian.hpp>
#include "dataconvert.h"
#include "writeengine.h"

#if defined(_MSC_VER) && defined(xxxDDLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

class WE_DDLCommandProc
{
	public:
		enum LogFileType { DROPTABLE_LOG, DROPPART_LOG, TRUNCATE_LOG};
		EXPORT WE_DDLCommandProc();
		EXPORT WE_DDLCommandProc(const WE_DDLCommandProc& rhs);
		EXPORT ~WE_DDLCommandProc();
		/** @brief Update SYSCOLUMN nextval column for the columnoid with nextVal.
		*
		* Update SYSCOLUMN nextval column for the columnoid with nexValue.
		* @param columnOid (in) The column OID
		* @param nextVal (in) The partition number
		* @return 0 on success, non-0 on error.
		*/		
		EXPORT uint8_t updateSyscolumnNextval(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t writeSystable(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t writeSyscolumn(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t createtablefiles(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t commitVersion(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t rollbackBlocks(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t rollbackVersion(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t deleteSyscolumn(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t deleteSyscolumnRow(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t deleteSystable(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t deleteSystables(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t dropFiles(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnAuto(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnNextvalCol(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnTablename(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSystableAuto(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSystableTablename(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSystablesTablename(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnColumnposCol(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t fillNewColumn(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnRenameColumn(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t updateSyscolumnSetDefault(messageqcpp::ByteStream& bs, std::string & err);
	//	EXPORT uint8_t updateSyscolumn(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t writeTruncateLog(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t writeDropPartitionLog(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t writeDropTableLog(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t deleteDDLLog(messageqcpp::ByteStream& bs, std::string & err);
		EXPORT uint8_t fetchDDLLog(messageqcpp::ByteStream& bs, std::string & err);
		void purgeFDCache();
		/** @brief drop a set of partitions
		 *
		 * Do many VSS lookups under one lock.
		 * @param bs (in) bytestream carry command info
		 * @param err (out) error message when error occurs
		 * @return 0 on success, otherwise error.
		 */
		EXPORT uint8_t dropPartitions(messageqcpp::ByteStream& bs, std::string& err);
		inline void convertRidToColumn (uint64_t& rid, uint16_t& dbRoot, uint32_t& partition, uint16_t& segment, const int32_t oid )
		{
			fDbrm.getSysCatDBRoot(oid, dbRoot);
			partition = rid / ( filesPerColumnPartition * extentsPerSegmentFile * extentRows );
		
			segment =( ( ( rid % ( filesPerColumnPartition * extentsPerSegmentFile * extentRows )) / extentRows ) ) % filesPerColumnPartition;
		
			//Calculate the relative rid for this segment file
			uint64_t relRidInPartition = rid - ((uint64_t)partition * (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
			assert ( relRidInPartition <= (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile * (uint64_t)extentRows );
			uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
			unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
			uint64_t relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
			rid = relRidInThisExtent +  numExtentsInThisSegPart * extentRows;
		}

	private:	
		WriteEngineWrapper fWEWrapper;
		BRM::DBRM fDbrm;
		unsigned  extentsPerSegmentFile, extentRows, filesPerColumnPartition, dbrootCnt;
	
};
}
#undef EXPORT
#endif
