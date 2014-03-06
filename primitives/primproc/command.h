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

#ifndef COMMAND_H
#define COMMAND_H

#include <boost/shared_ptr.hpp>
#include <boost/uuid/uuid.hpp>

#include "serializeable.h"
#include "bytestream.h"
#include "rowgroup.h"

namespace primitiveprocessor
{

class BatchPrimitiveProcessor;
class Command;
typedef boost::shared_ptr<Command> SCommand;

class Command
{
public:
	enum CommandType {
		NONE,
		COLUMN_COMMAND,
		DICT_STEP,
		DICT_SCAN,
		PASS_THRU,
		RID_TO_STRING,
		FILTER_COMMAND,
        PSEUDOCOLUMN
	};

	static const uint8_t NOT_FEEDER = 0;
	static const uint8_t FILT_FEEDER = 1;
	static const uint8_t LEFT_FEEDER = 3;
	static const uint8_t RIGHT_FEEDER = 5;

	Command(CommandType c);
	virtual ~Command();

	virtual void execute() = 0;
	virtual void project() = 0;
	virtual void projectIntoRowGroup(rowgroup::RowGroup &rg, uint32_t columnPosition) = 0;
	virtual uint64_t getLBID() = 0;
	virtual void getLBIDList(uint32_t loopCount, std::vector<int64_t> *out) {}  // the default fcn returns 0 lbids
	virtual void nextLBID() = 0;
	virtual void createCommand(messageqcpp::ByteStream &);
	virtual void resetCommand(messageqcpp::ByteStream &);

	/* Duplicate() makes a copy of this object as constructed by createCommand().
		It's thread-safe */
	virtual SCommand duplicate() = 0;
	bool operator==(const Command &) const;
	bool operator!=(const Command& c) const { return !(*this == c); }

	/* Put bootstrap code here (ie, build the template primitive msg) */
	virtual void prep(int8_t outputType, bool makeAbsRids) = 0;
	virtual void setBatchPrimitiveProcessor(BatchPrimitiveProcessor *);
	virtual void setMakeAbsRids(bool);

	CommandType getCommandType() const { return cmdType; }

	// if feeding a filtercommand
	// note: a filtercommand itself can feed another filtercommand
	uint8_t filterFeeder()       { return fFilterFeeder; }
	void filterFeeder(uint8_t f) { fFilterFeeder = f; }
	virtual void copyRidsForFilterCmd();

	static Command* makeCommand(messageqcpp::ByteStream&, CommandType*, std::vector<SCommand>&);

	uint32_t getOID() const { return OID; }
	uint32_t getTupleKey() const { return tupleKey; }

	virtual int getCompType() const=0;

protected:
	BatchPrimitiveProcessor *bpp;
	CommandType cmdType;
	uint8_t fFilterFeeder;
	uint32_t OID;
	uint32_t tupleKey;
	boost::uuids::uuid queryUuid;
	boost::uuids::uuid stepUuid;

	void duplicate(Command *);

private:
	Command();
	Command(const Command &);

};

}

#endif
