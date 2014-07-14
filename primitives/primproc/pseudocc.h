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


#ifndef PSEUDOCC_H
#define PSEUDOCC_H

#include "columncommand.h"
#include "blocksize.h"

namespace primitiveprocessor {

class PseudoCC : public ColumnCommand
{
public:
	PseudoCC();
	virtual ~PseudoCC();

	virtual SCommand duplicate();
	virtual void createCommand(messageqcpp::ByteStream &);
	virtual void resetCommand(messageqcpp::ByteStream &);
protected:
	virtual void loadData();

private:

	template<typename W> void loadSingleValue(W val);
	template<typename W> void loadPMNumber();
	template<typename W> void loadRIDs();
	template<typename W> void loadSegmentNum();
	template<typename W> void loadDBRootNum();
	template<typename W> void loadPartitionNum();
	template<typename W> void loadLBID();

	uint32_t function;
	uint64_t valueFromUM;
};


template<typename W>
#ifdef __GNUC__
// The "official" GCC version for InfiniDB is 4.5.3. This flag breaks at least 4.4.3 & 4.4.5,
//   so just don't do it for anything younger than 4.5.0.
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 5)
__attribute__((optimize("no-tree-vectorize")))
#endif
#endif
void PseudoCC::loadSingleValue(W val)
{
	W *bData = (W *) bpp->blockData;
	for (uint32_t i = 0; i < BLOCK_SIZE; i++)
		bData[i] = val;   // this breaks when using tree-vectorization
	//memcpy(&bData[i], &val, sizeof(W));   // this always works
}

template<typename W>
void PseudoCC::loadPMNumber()
{
	uint32_t pmNum = oamCache->getLocalPMId();
	loadSingleValue<W>(pmNum);
}

/* This returns extent-relative rids */
template<typename W>
void PseudoCC::loadRIDs()
{
	uint32_t i;
	uint64_t baseRid = rowgroup::getExtentRelativeRid(bpp->baseRid);
	W *bData = (W *) bpp->blockData;
	for (i = 0; i < BLOCK_SIZE; i++) {
		//W val = baseRid + i;
		//memcpy(&bData[i], &val, sizeof(W));
		bData[i] = baseRid + i;    // breaks with tree-vectorization
	}
}

template<typename W>
void PseudoCC::loadSegmentNum()
{
	uint16_t segNum;
	rowgroup::getLocationFromRid(bpp->baseRid, NULL, &segNum, NULL, NULL);
	loadSingleValue<W>(segNum);
}

template<typename W>
void PseudoCC::loadPartitionNum()
{
	uint32_t partNum;
	rowgroup::getLocationFromRid(bpp->baseRid, &partNum, NULL, NULL, NULL);
	loadSingleValue<W>(partNum);
}

template<typename W>
void PseudoCC::loadDBRootNum()
{
	loadSingleValue<W>(bpp->dbRoot);
}

template<typename W>
void PseudoCC::loadLBID()
{
	uint32_t i;
	W *bData = (W *) bpp->blockData;
	for (i = 0; i < BLOCK_SIZE; i++) {
		//W val = ColumnCommand::getLBID() + (i * sizeof(W)) / BLOCK_SIZE;
		//memcpy(&bData[i], &val, sizeof(W));
		bData[i] = ColumnCommand::getLBID() + (i * sizeof(W)) / BLOCK_SIZE;  // breaks with tree-vectorization
	}
}

}

#endif // PseudoCC_H
