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

/** @file */

#ifndef COMMANDJL_H
#define COMMANDJL_H

#include <string>
#include <boost/uuid/uuid.hpp>

#include "serializeable.h"
#include "bytestream.h"

namespace joblist
{

class BatchPrimitiveProcessorJL;

class CommandJL
{
public:
	CommandJL();
	CommandJL(const CommandJL &);
	virtual ~CommandJL();

	// converts a rid or dictionary token to an LBID.  For ColumnCommandJL it's a RID,
	// for a DictStep it's a token.
	virtual void setLBID(uint64_t data, uint32_t dbroot) = 0;
	virtual uint8_t getTableColumnType() = 0;
	virtual std::string toString() = 0;
	virtual void createCommand(messageqcpp::ByteStream &) const;
	virtual void runCommand(messageqcpp::ByteStream &) const = 0;
	uint32_t getOID() const { return OID; }
	const std::string& getColName() const { return colName; }
	void setTupleKey(uint32_t tkey) { tupleKey = tkey; }
	uint32_t getTupleKey() const { return tupleKey; }
	virtual uint16_t getWidth() = 0;
	const boost::uuids::uuid& getQueryUuid() const { return queryUuid; }
	void setQueryUuid(const boost::uuids::uuid& u) { queryUuid = u; }
	const boost::uuids::uuid& getStepUuid() const { return stepUuid; }
	void setStepUuid(const boost::uuids::uuid& u) { stepUuid = u; }

	void setBatchPrimitiveProcessor(BatchPrimitiveProcessorJL* b) { bpp = b; }

	enum CommandType {
		NONE,
		COLUMN_COMMAND,
		DICT_STEP,
		DICT_SCAN,
		PASS_THRU,
		RID_TO_STRING,
		FILTER_COMMAND,
		PSEUDO_COLUMN
	};

	virtual CommandType getCommandType() = 0;

protected:
	BatchPrimitiveProcessorJL *bpp;
	uint32_t OID;
	uint32_t tupleKey;
	std::string colName;  // for stats
	boost::uuids::uuid queryUuid;
	boost::uuids::uuid stepUuid;

private:

};

typedef boost::shared_ptr<CommandJL> SCommand;

}

#endif
