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


#include "bpp.h"
#include "pseudocolumn.h"

using namespace std;
using namespace execplan;

namespace primitiveprocessor {

PseudoCC::PseudoCC() : ColumnCommand()
{
}

PseudoCC::~PseudoCC()
{
}

SCommand PseudoCC::duplicate()
{
	SCommand ret;
	PseudoCC *pseudo;

	pseudo = new PseudoCC();
	ret.reset(pseudo);
	pseudo->function = function;
	pseudo->valueFromUM = valueFromUM;
	ColumnCommand::duplicate(pseudo);
	return ret;
}

void PseudoCC::createCommand(messageqcpp::ByteStream &bs)
{
	bs.advance(1);
	bs >> function;
	ColumnCommand::createCommand(bs);
}

void PseudoCC::resetCommand(messageqcpp::ByteStream &bs)
{
	if (function == PSEUDO_EXTENTMAX || function == PSEUDO_EXTENTMIN || function == PSEUDO_EXTENTID)
		bs >> valueFromUM;
	ColumnCommand::resetCommand(bs);
}

#ifdef __GNUC__
// The "official" GCC version for InfiniDB is 4.5.3. This flag breaks at least 4.4.3 & 4.4.5,
//   so just don't do it for anything younger than 4.5.0.
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 5)
__attribute__((optimize("no-tree-vectorize")))
#endif
#endif

void PseudoCC::loadData()
{
	switch (function) {
	case PSEUDO_PM:
		switch(colType.colWidth) {
		case 1:
			loadPMNumber<uint8_t>();
			break;
		case 2:
			loadPMNumber<uint16_t>();
			break;
		case 4:
			loadPMNumber<uint32_t>();
			break;
		case 8:
			loadPMNumber<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	case PSEUDO_EXTENTRELATIVERID:
		switch(colType.colWidth) {
		case 1:
			loadRIDs<uint8_t>();
			break;
		case 2:
			loadRIDs<uint16_t>();
			break;
		case 4:
			loadRIDs<uint32_t>();
			break;
		case 8:
			loadRIDs<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	case PSEUDO_SEGMENT:
		switch(colType.colWidth) {
		case 1:
			loadSegmentNum<uint8_t>();
			break;
		case 2:
			loadSegmentNum<uint16_t>();
			break;
		case 4:
			loadSegmentNum<uint32_t>();
			break;
		case 8:
			loadSegmentNum<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	case PSEUDO_SEGMENTDIR:
		switch(colType.colWidth) {
		case 1:
			loadPartitionNum<uint8_t>();
			break;
		case 2:
			loadPartitionNum<uint16_t>();
			break;
		case 4:
			loadPartitionNum<uint32_t>();
			break;
		case 8:
			loadPartitionNum<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	case PSEUDO_BLOCKID:
		switch(colType.colWidth) {
		case 1:
			loadLBID<uint8_t>();
			break;
		case 2:
			loadLBID<uint16_t>();
			break;
		case 4:
			loadLBID<uint32_t>();
			break;
		case 8:
			loadLBID<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	case PSEUDO_DBROOT:
		switch(colType.colWidth) {
		case 1:
			loadDBRootNum<uint8_t>();
			break;
		case 2:
			loadDBRootNum<uint16_t>();
			break;
		case 4:
			loadDBRootNum<uint32_t>();
			break;
		case 8:
			loadDBRootNum<uint64_t>();
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
		/* cases where the value to use is sent from the UM */
	case PSEUDO_EXTENTMAX:
	case PSEUDO_EXTENTMIN:
	case PSEUDO_EXTENTID:
		switch(colType.colWidth) {
		case 1:
			loadSingleValue<int8_t>(valueFromUM);
			break;
		case 2:
			loadSingleValue<int16_t>(valueFromUM);
			break;
		case 4:
			loadSingleValue<int32_t>(valueFromUM);
			break;
		case 8:
			loadSingleValue<int64_t>(valueFromUM);
			break;
		default:
			cout << "PC::loadData(): bad column width" << endl;
			break;
		}
		break;
	default:
		cout << "PC::loadData(): bad function" << endl;
		break;
	}
}

}
