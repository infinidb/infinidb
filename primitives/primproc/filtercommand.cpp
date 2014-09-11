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

/***********************************************************************
*   $Id: filtercommand.cpp 1588 2011-02-08 14:37:03Z rdempsey $
*
*
***********************************************************************/

#include <sstream>
#include "primitiveserver.h"
#include "columncommand.h"
#include "dictstep.h"
#include "filtercommand.h"
#include "dataconvert.h"

using namespace std;
using namespace messageqcpp;

namespace
{
uint32_t cc = primitiveprocessor::Command::COLUMN_COMMAND;
uint32_t ds = primitiveprocessor::Command::DICT_STEP;
const uint32_t CC   = (cc << 8)  | cc;
const uint32_t DCC  = (ds << 16) | (cc << 8)  | cc;
const uint32_t CDC  = (cc << 16) | (ds << 8)  | cc;
const uint32_t DCDC = (ds << 24) | (cc << 16) | (ds << 8) | cc;

inline bool isNull(int64_t val, const execplan::CalpontSystemCatalog::ColType& ct)
{
	bool ret = false;

	switch (ct.colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		{
			if ((int8_t) joblist::TINYINTNULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::CHAR:
		{
			int colWidth = ct.colWidth;
			if (colWidth <= 8)
			{
				if ((colWidth == 1) && ((int8_t) joblist::CHAR1NULL == val)) ret = true ;
				else if ((colWidth == 2) && ((int16_t) joblist::CHAR2NULL == val)) ret = true;
				else if ((colWidth < 5) && ((int32_t) joblist::CHAR4NULL == val)) ret = true;
				else if ((int64_t) joblist::CHAR8NULL == val) ret = true;
			}
			else
			{
				throw logic_error("Not a int column.");
			}
			break;
		}
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			if ((int16_t) joblist::SMALLINTNULL == val) ret = true;
			break;
		}
		//TODO: does DECIMAL belong here?
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::DOUBLE:
		{
			if ((int64_t) joblist::DOUBLENULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		{
			if ((int32_t) joblist::INTNULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::FLOAT:
		{
			if ((int32_t) joblist::FLOATNULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			if ((int32_t) joblist::DATENULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::BIGINT:
		{
			if ((int64_t) joblist::BIGINTNULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			if ((int64_t) joblist::DATETIMENULL == val) ret = true;
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			int colWidth = ct.colWidth;
			if (colWidth <= 8)
			{
				if ((colWidth < 3) && ((int16_t) joblist::CHAR2NULL == val)) ret = true;
				else if ((colWidth < 5) && ((int32_t) joblist::CHAR4NULL == val)) ret = true;
				else if ((int64_t) joblist::CHAR8NULL == val) ret = true;
			}
			else
			{
				throw logic_error("Not a int column.");
			}
			break;
		}
		default:
			break;
	}

	return ret;
}

};


namespace primitiveprocessor
{

Command* FilterCommand::makeFilterCommand(ByteStream& bs, vector<SCommand>& cmds)
{
	// skip the command type byte -- filtercommand
	bs.advance(1);

	// find out the # of commands in the cmds vector,
	// vector::size() will not work, because cmds is resize() to filterCount
	uint64_t nc = 0;
	while (cmds[nc].get() != NULL) nc++;

	// figure out the feeding commands
	// must have 2 columncommands, may have 1 or 2 dictsteps.
	uint64_t cols = 0;			// # of columncommands
	uint32_t columns = 0;
	uint64_t i = nc;
	while (i > 0 && cols < 2)
	{
		Command::CommandType cmdType = cmds[i-1]->getCommandType();
		if (cmdType != Command::COLUMN_COMMAND && cmdType != Command::DICT_STEP)
		{
			stringstream msg;
			msg << "FilterCommand: feeded by " << cmdType << " is not supported.";
			throw logic_error(msg.str());
		}

		columns = (columns<<8) + cmdType;
		if (cmdType == Command::COLUMN_COMMAND)
			cols++;
		i--;
	}

	// should not happen
	if (cols < 2) throw logic_error("FilterCommand: not enough feeding ColumnCommands.");


    FilterCommand* fc = NULL;
	// the order setting left/right feeder is important, left is the smaller index.
	// because the doFilter relies on the rids of right-feeder is a subset of left.
	if (columns == CC)
	{
		cmds[nc-2]->filterFeeder(LEFT_FEEDER);
		cmds[nc-1]->filterFeeder(RIGHT_FEEDER);

		ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc-2].get());
		ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc-1].get());
		int scale0 = cmd0->getScale();
		int scale1 = cmd1->getScale();
		// char[] is stored as int, but cannot directly compare if length is different
		// due to endian issue
		if (cmd0->getColType().colDataType == execplan::CalpontSystemCatalog::CHAR ||
			cmd0->getColType().colDataType == execplan::CalpontSystemCatalog::VARCHAR)
		{
			StrFilterCmd* sc = new StrFilterCmd();
			sc->setCompareFunc(CC);
			fc = sc;
		}
		else if (scale0 == scale1)
		{
			fc = new FilterCommand();
		}
		else
		{
			ScaledFilterCmd* sc = new ScaledFilterCmd();
			sc->setFactor(pow(10.0, scale1) / pow(10.0, scale0));
			fc = sc;
		}

		fc->setColTypes(cmd0->getColType(), cmd1->getColType());
	}
	else if (columns == DCDC)  // both string
	{
		StrFilterCmd* sc = new StrFilterCmd();
		cmds[nc-4]->filterFeeder(FILT_FEEDER);
		cmds[nc-3]->filterFeeder(LEFT_FEEDER);
		cmds[nc-2]->filterFeeder(FILT_FEEDER);
		cmds[nc-1]->filterFeeder(RIGHT_FEEDER);
		sc->setCompareFunc(DCDC);
		fc = sc;

		ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc-4].get());
		ColumnCommand* cmd2 = dynamic_cast<ColumnCommand*>(cmds[nc-2].get());
		fc->setColTypes(cmd0->getColType(), cmd2->getColType());
	}
	else if (columns == DCC)  // lhs: char[]; rhs: string
	{
		StrFilterCmd* sc = new StrFilterCmd();
		cmds[nc-3]->filterFeeder(LEFT_FEEDER);
		cmds[nc-2]->filterFeeder(FILT_FEEDER);
		cmds[nc-1]->filterFeeder(RIGHT_FEEDER);
		ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc-3].get());
		ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc-2].get());
		size_t cl = cmd0->getWidth(); // char[] column 
		sc->setCharLength(cl);
		sc->setCompareFunc(DCC);
		fc = sc;
		fc->setColTypes(cmd0->getColType(), cmd1->getColType());
	}
	else if (columns == CDC)  // lhs: string; rhs: char[]
	{
		StrFilterCmd* sc = new StrFilterCmd();
		cmds[nc-3]->filterFeeder(FILT_FEEDER);
		cmds[nc-2]->filterFeeder(LEFT_FEEDER);
		cmds[nc-1]->filterFeeder(RIGHT_FEEDER);
		ColumnCommand* cmd0 = dynamic_cast<ColumnCommand*>(cmds[nc-3].get());
		ColumnCommand* cmd1 = dynamic_cast<ColumnCommand*>(cmds[nc-1].get());
		size_t cl = cmd1->getWidth(); // char[] column
		sc->setCharLength(cl);
		sc->setCompareFunc(CDC);
		fc = sc;
		fc->setColTypes(cmd0->getColType(), cmd1->getColType());
	}
	else
	{
		stringstream msg;
		msg << "FilterCommand does not handle this column code: " << hex << columns << dec;
		throw logic_error(msg.str());
	}

	return fc;
}


FilterCommand::FilterCommand() : Command(FILTER_COMMAND), fBOP(0)
{
}


FilterCommand::~FilterCommand()
{
}


void FilterCommand::execute()
{
	doFilter();
}


void FilterCommand::createCommand(ByteStream& bs)
{
	bs >> fBOP;
	Command::createCommand(bs);
}


void FilterCommand::resetCommand(ByteStream &bs)
{
}


void FilterCommand::prep(int8_t outputType, bool absRids)
{
}


void FilterCommand::project()
{
}

void FilterCommand::projectIntoRowGroup(rowgroup::RowGroup &rg, uint col)
{
}


uint64_t FilterCommand::getLBID()
{
	return 0;
}


void FilterCommand::nextLBID()
{
}


SCommand FilterCommand::duplicate()
{
	SCommand ret;
	FilterCommand* filterCmd;

	ret.reset(new FilterCommand());
	filterCmd = (FilterCommand *) ret.get();
	filterCmd->fBOP = fBOP;
	filterCmd->leftColType = leftColType;
	filterCmd->rightColType = rightColType;
	filterCmd->Command::duplicate(this);
	return ret;
}


void FilterCommand::setColTypes(const execplan::CalpontSystemCatalog::ColType& left,
								const execplan::CalpontSystemCatalog::ColType& right)
{
	leftColType = left;
	rightColType = right;
}


void FilterCommand::doFilter()
{
	bpp->ridMap = 0;
	bpp->ridCount = 0;

	// rids in [0] is used for scan [1], so [1] is a subset of [0], and same order.
	// -- see makeFilterCommand() above. 
	for (uint64_t i = 0, j = 0; j < bpp->fFiltRidCount[1];  )
	{
		if (bpp->fFiltCmdRids[0][i] != bpp->fFiltCmdRids[1][j])
		{
			i++;
		}
		else
		{
			if (compare(i,j) == true)
			{
				bpp->relRids[bpp->ridCount] = bpp->fFiltCmdRids[0][i];
				bpp->values[bpp->ridCount] = bpp->fFiltCmdValues[0][i];
				bpp->ridMap |= 1 << (bpp->relRids[bpp->ridCount] >> 10);
				bpp->ridCount++;
			}

			i++;
			j++;
		}
	}

	// bug 1247 -- reset the rid count
	bpp->fFiltRidCount[0] = bpp->fFiltRidCount[1] = 0;
}


bool FilterCommand::compare(uint64_t i, uint64_t j)
{
	if (isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
		isNull(bpp->fFiltCmdValues[1][j], rightColType))
		return false;

	switch(fBOP)
	{
		case COMPARE_GT:
			return bpp->fFiltCmdValues[0][i] > bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_LT:
			return bpp->fFiltCmdValues[0][i] < bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_EQ:
			return bpp->fFiltCmdValues[0][i] == bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_GE:
			return bpp->fFiltCmdValues[0][i] >= bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_LE:
			return bpp->fFiltCmdValues[0][i] <= bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_NE:
			return bpp->fFiltCmdValues[0][i] != bpp->fFiltCmdValues[1][j];
			break;
		default:
			return false;
			break;
	}
}


bool FilterCommand::operator==(const FilterCommand& c) const
{
	return (fBOP == c.fBOP);
}


bool FilterCommand::operator!=(const FilterCommand& c) const
{
	return !(*this == c);
}


// == ScaledFilterCmd ==
ScaledFilterCmd::ScaledFilterCmd() : fFactor(1)
{
}


ScaledFilterCmd::~ScaledFilterCmd()
{
}


SCommand ScaledFilterCmd::duplicate()
{
	SCommand ret;
	ScaledFilterCmd* filterCmd;

	ret.reset(new ScaledFilterCmd());
	filterCmd = (ScaledFilterCmd *) ret.get();
	filterCmd->fBOP = fBOP;
	filterCmd->fFactor = fFactor;

	return ret;
}


bool ScaledFilterCmd::compare(uint64_t i, uint64_t j)
{
	if (isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
		isNull(bpp->fFiltCmdValues[1][j], rightColType))
		return false;

	switch(fBOP)
	{
		case COMPARE_GT:
			return bpp->fFiltCmdValues[0][i]*fFactor > bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_LT:
			return bpp->fFiltCmdValues[0][i]*fFactor < bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_EQ:
			return bpp->fFiltCmdValues[0][i]*fFactor == bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_GE:
			return bpp->fFiltCmdValues[0][i]*fFactor >= bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_LE:
			return bpp->fFiltCmdValues[0][i]*fFactor <= bpp->fFiltCmdValues[1][j];
			break;
		case COMPARE_NE:
			return bpp->fFiltCmdValues[0][i]*fFactor != bpp->fFiltCmdValues[1][j];
			break;
		default:
			return false;
			break;
	}
}


void ScaledFilterCmd::setFactor(double f)
{
	fFactor = f;
}


double ScaledFilterCmd::factor()
{
	return fFactor;
}


bool ScaledFilterCmd::operator==(const ScaledFilterCmd& c) const
{
	return ((fBOP == c.fBOP) && (fFactor == c.fFactor));
}


bool ScaledFilterCmd::operator!=(const ScaledFilterCmd& c) const
{
	return !(*this == c);
}


// == StrFilterCmd ==
StrFilterCmd::StrFilterCmd() : fCompare(NULL), fCharLength(8)
{
}


StrFilterCmd::~StrFilterCmd()
{
}


SCommand StrFilterCmd::duplicate()
{
	SCommand ret;
	StrFilterCmd* filterCmd;

	ret.reset(new StrFilterCmd());
	filterCmd = (StrFilterCmd *) ret.get();
	filterCmd->fBOP = fBOP;
	filterCmd->fCompare = fCompare;
	filterCmd->fCharLength = fCharLength;

	return ret;
}


void StrFilterCmd::execute()
{
	doFilter();
}


bool StrFilterCmd::compare(uint64_t i, uint64_t j)
{
	return (this->*fCompare)(i, j);
}


void StrFilterCmd::setCompareFunc(uint32_t columns)
{
	if (columns == CC)        // char[] : char
	{
		fCompare = &StrFilterCmd::compare_cc;
	}
	else if (columns == DCDC) // string : string
	{
		fCompare = &StrFilterCmd::compare_ss;
	}
	else if (columns == DCC)  // char[] : string
	{
			fCompare = &StrFilterCmd::compare_cs;
	}
	else if (columns == CDC)  // string : char[]
	{
			fCompare = &StrFilterCmd::compare_sc;
	}
	else
	{
			stringstream msg;
			msg << "StrFilterCmd: unhandled column combination " << hex << columns << dec;
			throw logic_error(msg.str());
	}
}


bool StrFilterCmd::compare_cc(uint64_t i, uint64_t j)
{
	if (isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
		isNull(bpp->fFiltCmdValues[1][j], rightColType))
		return false;

	switch(fBOP)
	{
		case COMPARE_GT:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) > uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		case COMPARE_LT:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) < uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		case COMPARE_EQ:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) == uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		case COMPARE_GE:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) >= uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		case COMPARE_LE:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) <= uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		case COMPARE_NE:
			return uint64ToStr(bpp->fFiltCmdValues[0][i]) != uint64ToStr(bpp->fFiltCmdValues[1][j]);
			break;
		default:
			return false;
			break;
	}
}


bool StrFilterCmd::compare_ss(uint64_t i, uint64_t j)
{
	if (bpp->fFiltStrValues[0][i] == "" || bpp->fFiltStrValues[1][j] == "" ||
		bpp->fFiltStrValues[0][i] == joblist::CPNULLSTRMARK || bpp->fFiltStrValues[1][j] == joblist::CPNULLSTRMARK)
		return false;

	switch(fBOP)
	{
		case COMPARE_GT:
			return bpp->fFiltStrValues[0][i] > bpp->fFiltStrValues[1][j];
			break;
		case COMPARE_LT:
			return bpp->fFiltStrValues[0][i] < bpp->fFiltStrValues[1][j];
			break;
		case COMPARE_EQ:
			return bpp->fFiltStrValues[0][i] == bpp->fFiltStrValues[1][j];
			break;
		case COMPARE_GE:
			return bpp->fFiltStrValues[0][i] >= bpp->fFiltStrValues[1][j];
			break;
		case COMPARE_LE:
			return bpp->fFiltStrValues[0][i] <= bpp->fFiltStrValues[1][j];
			break;
		case COMPARE_NE:
			return bpp->fFiltStrValues[0][i] != bpp->fFiltStrValues[1][j];
			break;
		default:
			return false;
			break;
	}
}


bool StrFilterCmd::compare_cs(uint64_t i, uint64_t j)
{
	if (isNull(bpp->fFiltCmdValues[0][i], leftColType) ||
		bpp->fFiltStrValues[1][j] == "" || bpp->fFiltStrValues[1][j] == joblist::CPNULLSTRMARK)
		return false;

	int cmp = strncmp(reinterpret_cast<const char*>(&bpp->fFiltCmdValues[0][i]),
						bpp->fFiltStrValues[1][j].c_str(), fCharLength);
	switch(fBOP)
	{
		case COMPARE_GT:
			return (cmp > 0);
			break;
		case COMPARE_LT:
			return (cmp < 0 || (cmp == 0 && fCharLength < bpp->fFiltStrValues[1][j].length()));
			break;
		case COMPARE_EQ:
			return (cmp == 0 && fCharLength >= bpp->fFiltStrValues[1][j].length());
			break;
		case COMPARE_GE:
			return (cmp > 0 || (cmp == 0 && fCharLength >= bpp->fFiltStrValues[1][j].length()));
			break;
		case COMPARE_LE:
			return (cmp <= 0);
			break;
		case COMPARE_NE:
			return (cmp != 0 || fCharLength < bpp->fFiltStrValues[1][j].length());
			break;
		default:
			return false;
			break;
	}
}


bool StrFilterCmd::compare_sc(uint64_t i, uint64_t j)
{
	if (bpp->fFiltStrValues[0][i] == "" || bpp->fFiltStrValues[0][i] == joblist::CPNULLSTRMARK ||
		isNull(bpp->fFiltCmdValues[1][j], rightColType))
		return false;

	int cmp = strncmp(bpp->fFiltStrValues[0][i].c_str(),
						reinterpret_cast<const char*>(&bpp->fFiltCmdValues[1][j]), fCharLength);

	switch(fBOP)
	{
		case COMPARE_GT:
			return (cmp > 0 || (cmp == 0 && bpp->fFiltStrValues[0][i].length() > fCharLength));
			break;
		case COMPARE_LT:
			return (cmp < 0);
			break;
		case COMPARE_EQ:
			return (cmp == 0 && bpp->fFiltStrValues[0][i].length() <= fCharLength);
			break;
		case COMPARE_GE:
			return (cmp >= 0);
			break;
		case COMPARE_LE:
			return (cmp < 0 || (cmp == 0 && bpp->fFiltStrValues[0][i].length() <= fCharLength));
			break;
		case COMPARE_NE:
			return (cmp != 0 || bpp->fFiltStrValues[0][i].length() > fCharLength);
			break;
		default:
			return false;
			break;
	}
}


void StrFilterCmd::setCharLength(size_t l)
{
	fCharLength = l;
}


size_t StrFilterCmd::charLength()
{
	return fCharLength;
}


bool StrFilterCmd::operator==(const StrFilterCmd& c) const
{
	return ((fBOP == c.fBOP) && fCharLength == c.fCharLength);
}


bool StrFilterCmd::operator!=(const StrFilterCmd& c) const
{
	return !(*this == c);
}


};
