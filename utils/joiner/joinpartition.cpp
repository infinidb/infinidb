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

#define _CRT_RAND_S //for win rand_s
#include <unistd.h>
#include <boost/filesystem.hpp>
#include "configcpp.h"
#include "joinpartition.h"
#include "tuplejoiner.h"
#include "atomicops.h"

using namespace std;
using namespace utils;
using namespace rowgroup;
using namespace messageqcpp;
using namespace logging;

#ifdef _MSC_VER
namespace {
	// returns a value between 0 and RAND_MAX (on Linux this is the max signed int)
	int rand_r(unsigned int* seedp)
	{
		unsigned int randval=0;
		rand_s(&randval); //returns 0 if okay, 0<=randval<UINT_MAX
		// should implicitly cast to signed okay here...
		return randval;
	}
}
#endif

namespace joiner {

uint64_t uniqueNums = 0;

JoinPartition::JoinPartition() { }

/* This is the ctor used by THJS */
JoinPartition::JoinPartition(const RowGroup &lRG,
		const RowGroup &sRG,
		const vector<uint32_t> &smallKeys,
		const vector<uint32_t> &largeKeys,
		bool typeless,
		bool antiWMN,
		bool hasFEFilter,
		uint64_t totalUMMemory,
		uint64_t partitionSize) :
			smallRG(sRG), largeRG(lRG),
			smallKeyCols(smallKeys), largeKeyCols(largeKeys), typelessJoin(typeless),
			nextPartitionToReturn(0), htSizeEstimate(0), htTargetSize(partitionSize), rootNode(true),
			antiWithMatchNulls(antiWMN), needsAllNullRows(hasFEFilter), gotNullRow(false),
			totalBytesRead(0), totalBytesWritten(0), maxLargeSize(0), maxSmallSize(0),
			nextSmallOffset(0), nextLargeOffset(0)
{
	config::Config *config = config::Config::makeConfig();
	string cfgTxt;
	smallSizeOnDisk = largeSizeOnDisk = 0;

	/* Debugging, rand() is used to simulate failures
	time_t t = time(NULL);
	srand(t);
	*/


	cfgTxt = config->getConfig("HashJoin", "TempFileCompression");
	if (cfgTxt == "n" || cfgTxt == "N")
		useCompression = false;
	else
		useCompression = true;

	fileMode = false;
	uniqueID = atomicops::atomicInc(&uniqueNums);
	uint32_t tmp = uniqueID;
	hashSeed = rand_r(&tmp);
	hashSeed = hasher((char *) &hashSeed, sizeof(hashSeed), uniqueID);
	hashSeed = hasher.finalize(hashSeed, sizeof(hashSeed));

	// start with initial capacity = 2 * totalUMMemory
	bucketCount = (totalUMMemory * 2)/htTargetSize + 1;

	largeRG.initRow(&largeRow);
	smallRG.initRow(&smallRow);

	buckets.reserve(bucketCount);
	for (int i = 0; i < (int) bucketCount; i++)
		buckets.push_back(boost::shared_ptr<JoinPartition>(new JoinPartition(*this, false)));
}

/* Ctor used by JoinPartition on expansion, creates JP's in filemode */
JoinPartition::JoinPartition(const JoinPartition &jp, bool splitMode) :
			smallRG(jp.smallRG), largeRG(jp.largeRG),
			smallKeyCols(jp.smallKeyCols), largeKeyCols(jp.largeKeyCols),
			typelessJoin(jp.typelessJoin), bucketCount(jp.bucketCount),
			smallRow(jp.smallRow), largeRow(jp.largeRow),
			nextPartitionToReturn(0), htSizeEstimate(0), htTargetSize(jp.htTargetSize),
			rootNode(false), antiWithMatchNulls(jp.antiWithMatchNulls),
			needsAllNullRows(jp.needsAllNullRows), gotNullRow(false),
			useCompression(jp.useCompression), totalBytesRead(0),
			totalBytesWritten(0), maxLargeSize(0), maxSmallSize(0),
			nextSmallOffset(0), nextLargeOffset(0)
{
	config::Config *config = config::Config::makeConfig();
	boost::posix_time::ptime t;
	ostringstream os;

	fileMode = true;
	// tuning issue: with the defaults, each 100MB bucket would split s.t. the children
	// could store another 4GB total.  Given a good hash and evenly distributed data,
	// the first level of expansion would happen for all JPs at once, giving a total
	// capacity of (4GB * 40) = 160GB, when actual usage at that point is a little over 4GB.
	// Instead, each will double in size, giving a capacity of 8GB -> 16 -> 32, and so on.
//	bucketCount = jp.bucketCount;
	bucketCount = 2;
	filenamePrefix = config->getConfig("HashJoin", "TempFilePath");
	if (filenamePrefix.empty())
		filenamePrefix = "/tmp/infinidb";

	filenamePrefix += "/Infinidb-join-data-";

	uniqueID = atomicops::atomicInc(&uniqueNums);
	uint32_t tmp = uniqueID;
	hashSeed = rand_r(&tmp);
	hashSeed = hasher((char *) &hashSeed, sizeof(hashSeed), uniqueID);
	hashSeed = hasher.finalize(hashSeed, sizeof(hashSeed));

	os << filenamePrefix << uniqueID;
	filenamePrefix = os.str();
	smallFilename = filenamePrefix + "-small";
	largeFilename = filenamePrefix + "-large";

	smallSizeOnDisk = largeSizeOnDisk = 0;

	buffer.reinit(smallRG);
	smallRG.setData(&buffer);
	smallRG.resetRowGroup(0);
	smallRG.getRow(0, &smallRow);
}


JoinPartition::~JoinPartition()
{
	if (fileMode) {
		smallFile.close();
		largeFile.close();
		boost::filesystem::remove(smallFilename);
		boost::filesystem::remove(largeFilename);
	}
}

int64_t JoinPartition::insertSmallSideRGData(RGData &rgData)
{
	int64_t ret;
	ret = processSmallBuffer(rgData);

	return ret;
}

int64_t JoinPartition::insertSmallSideRGData(vector<RGData> &rgData)
{
	int64_t ret = 0;

	// this iterates over the vector backward to free mem asap
	while (rgData.size() > 0) {
		ret += insertSmallSideRGData(rgData.back());
		rgData.pop_back();
	}

	return ret;
}

int64_t JoinPartition::insertSmallSideRow(const Row &row)
{
	int64_t ret = 0;

	copyRow(row, &smallRow);
	smallRG.incRowCount();

	if (smallRG.getRowCount() == 8192)
		ret = processSmallBuffer();
	else
		smallRow.nextRow();

	return ret;
}

int64_t JoinPartition::insertLargeSideRGData(RGData &rgData)
{
	int64_t ret;

	ret = processLargeBuffer(rgData);
	return ret;
}

int64_t JoinPartition::insertLargeSideRow(const Row &row)
{
	int64_t ret = 0;

	copyRow(row, &largeRow);
	largeRG.incRowCount();

	if (largeRG.getRowCount() == 8192)
		ret = processLargeBuffer();
	else
		largeRow.nextRow();

	return ret;
}

int64_t JoinPartition::doneInsertingSmallData()
{
	/*
		flush buffers to leaf nodes
		config for large-side insertion
	*/
	int64_t ret = 0;
	int64_t leafNodeIncrement;

	/* flushing doesn't apply to the root node b/c it inserts entire RGs at once */
	if (!rootNode)
		ret = processSmallBuffer();
	if (!fileMode)
		for (int i = 0; i < (int) buckets.size(); i++) {
			leafNodeIncrement = buckets[i]->doneInsertingSmallData();
			ret += leafNodeIncrement;
			smallSizeOnDisk += leafNodeIncrement;
		}
	//else
	//	cout << uniqueID << " htsizeestimate = " << htSizeEstimate << endl;

	if (!rootNode) {
		buffer.reinit(largeRG);
		largeRG.setData(&buffer);
		largeRG.resetRowGroup(0);
		largeRG.getRow(0, &largeRow);
	}

	if (maxSmallSize < smallSizeOnDisk)
		maxSmallSize = smallSizeOnDisk;

	return ret;
}

int64_t JoinPartition::doneInsertingLargeData()
{
	/*
		flush buffers to leaf nodes
	*/

	int64_t ret = 0;
	int64_t leafNodeIncrement;

	/* flushing doesn't apply to the root node b/c it inserts entire RGs at once */
	if (!rootNode)
		ret = processLargeBuffer();
	if (!fileMode)
		for (int i = 0; i < (int) buckets.size(); i++) {
			leafNodeIncrement = buckets[i]->doneInsertingLargeData();
			ret += leafNodeIncrement;
			largeSizeOnDisk += leafNodeIncrement;
		}

	if (maxLargeSize < largeSizeOnDisk)
		maxLargeSize = largeSizeOnDisk;

	return ret;
}

int64_t JoinPartition::convertToSplitMode()
{
	int i, j;
	ByteStream bs;
	RGData rgData;
	uint32_t hash;
	uint64_t tmp;
	int64_t ret = -(int64_t)smallSizeOnDisk;    // smallFile gets deleted
	boost::scoped_array<uint32_t> rowDist(new uint32_t[bucketCount]);
	uint32_t rowCount = 0;

	memset(rowDist.get(), 0, sizeof(uint32_t) * bucketCount);
	fileMode = false;
	htSizeEstimate = 0;
	smallSizeOnDisk = 0;
	buckets.reserve(bucketCount);
	for (i = 0; i < (int) bucketCount; i++)
		buckets.push_back(boost::shared_ptr<JoinPartition>(new JoinPartition(*this, false)));

	RowGroup &rg = smallRG;
	Row &row = smallRow;
	nextSmallOffset = 0;
	while (1) {
		readByteStream(0, &bs);
		if (bs.length() == 0)
			break;
		rgData.deserialize(bs);
		rg.setData(&rgData);
		for (j = 0; j < (int) rg.getRowCount(); j++) {
			rg.getRow(j, &row);

			if (antiWithMatchNulls && hasNullJoinColumn(row)) {
				if (needsAllNullRows || !gotNullRow) {
					for (j = 0; j < (int) bucketCount; j++)
						ret += buckets[j]->insertSmallSideRow(row);
					gotNullRow = true;
				}
				continue;
			}

			if (typelessJoin)
				hash = getHashOfTypelessKey(row, smallKeyCols, hashSeed) % bucketCount;
			else {
				if (UNLIKELY(row.isUnsigned(smallKeyCols[0])))
					tmp = row.getUintField(smallKeyCols[0]);
				else
					tmp = row.getIntField(smallKeyCols[0]);
				hash = hasher((char *) &tmp, 8, hashSeed);
				hash = hasher.finalize(hash, 8) % bucketCount;
			}
			rowCount++;
			rowDist[hash]++;
			ret += buckets[hash]->insertSmallSideRow(row);
		}
	}
	boost::filesystem::remove(smallFilename);
	smallFilename.clear();

	for (i = 0; i < (int) bucketCount; i++)
		if (rowDist[i] == rowCount)
			throw IDBExcept("All rows hashed to the same bucket", ERR_DBJ_DATA_DISTRIBUTION);

	rg.setData(&buffer);
	rg.resetRowGroup(0);
	rg.getRow(0, &row);

	return ret;
}

/* either forwards the specified buffer to the next level of JP's or
writes it to a file, returns the # of bytes written to the file */


int64_t JoinPartition::processSmallBuffer()
{
	int64_t ret;

	ret = processSmallBuffer(buffer);
	smallRG.resetRowGroup(0);
	smallRG.getRow(0, &smallRow);
	return ret;
}


int64_t JoinPartition::processSmallBuffer(RGData &rgData)
{
	RowGroup &rg = smallRG;
	Row &row = smallRow;
	int64_t ret = 0;

	rg.setData(&rgData);
	//if (rootNode)
		//cout << "smallside RGData: " << rg.toString() << endl;

	if (fileMode) {
		ByteStream bs;
		rg.serializeRGData(bs);
		//cout << "writing RGData: " << rg.toString() << endl;

		ret = writeByteStream(0, bs);
		//cout << "wrote " << ret << " bytes" << endl;

		/* Check whether this partition is now too big -> convert to split mode.

		The current estimate is based on 100M 4-byte rows = 4GB.  The total size is
		the amount stored in RowGroups in mem + the size of the hash table.  The RowGroups
		in that case use 600MB, so 3.4GB is used by the hash table.  3.4GB/100M rows = 34 bytes/row
		*/
		htSizeEstimate += rg.getDataSize() + (34 * rg.getRowCount());
		if (htSizeEstimate > htTargetSize)
			ret += convertToSplitMode();

		//cout << "wrote some data, returning " << ret << endl;
	}
	else {
		uint64_t hash, tmp;
		int i, j;

		for (i = 0; i < (int) rg.getRowCount(); i++) {
			rg.getRow(i, &row);

			if (antiWithMatchNulls && hasNullJoinColumn(row)) {
				if (needsAllNullRows || !gotNullRow) {
					for (j = 0; j < (int) bucketCount; j++)
						ret += buckets[j]->insertSmallSideRow(row);
					gotNullRow = true;
				}
				continue;
			}

			if (typelessJoin)
				hash = getHashOfTypelessKey(row, smallKeyCols, hashSeed) % bucketCount;
			else {
				if (UNLIKELY(row.isUnsigned(smallKeyCols[0])))
					tmp = row.getUintField(smallKeyCols[0]);
				else
					tmp = row.getIntField(smallKeyCols[0]);
				hash = hasher((char *) &tmp, 8, hashSeed);
				hash = hasher.finalize(hash, 8) % bucketCount;
			}
			//cout << "hashing smallside row: " << row.toString() << endl;
			ret += buckets[hash]->insertSmallSideRow(row);
		}
		//cout << "distributed rows, returning " << ret << endl;
	}
	smallSizeOnDisk += ret;
	return ret;
}


/* the difference between processSmall & processLarge is mostly the names of
   variables being small* -> large*, template? */

int64_t JoinPartition::processLargeBuffer()
{
	int64_t ret;

	ret = processLargeBuffer(buffer);
	largeRG.resetRowGroup(0);
	largeRG.getRow(0, &largeRow);
	return ret;
}

int64_t JoinPartition::processLargeBuffer(RGData &rgData)
{
	RowGroup &rg = largeRG;
	Row &row = largeRow;
	int64_t ret = 0;
	int i, j;

	rg.setData(&rgData);

	//if (rootNode)
	//	cout << "largeside RGData: "  << rg.toString() << endl;

	/* Need to fail a query with an anti join, an FE filter, and a NULL row on the
	large side b/c it needs to be joined with the entire small side table. */
	if (antiWithMatchNulls && needsAllNullRows) {
		rg.getRow(0, &row);
		for (i = 0; i < (int) rg.getRowCount(); i++, row.nextRow()) {
			for (j = 0; j < (int) largeKeyCols.size(); j++) {
				if (row.isNullValue(largeKeyCols[j]))
					throw QueryDataExcept("", ERR_DBJ_ANTI_NULL);
			}
		}
	}

	if (fileMode) {
		ByteStream bs;
		rg.serializeRGData(bs);
		//cout << "writing large RGData: " << rg.toString() << endl;
		ret = writeByteStream(1, bs);
		//cout << "wrote " << ret << " bytes" << endl;
	}
	else {
		uint64_t hash, tmp;
		int i;

		for (i = 0; i < (int) rg.getRowCount(); i++) {
			rg.getRow(i, &row);
			if (typelessJoin)
				hash = getHashOfTypelessKey(row, largeKeyCols, hashSeed) % bucketCount;
			else {
				if (UNLIKELY(row.isUnsigned(largeKeyCols[0])))
					tmp = row.getUintField(largeKeyCols[0]);
				else
					tmp = row.getIntField(largeKeyCols[0]);
				hash = hasher((char *) &tmp, 8, hashSeed);
				hash = hasher.finalize(hash, 8) % bucketCount;
			}
			//cout << "large side hashing row: " << row.toString() << endl;
			ret += buckets[hash]->insertLargeSideRow(row);
		}
	}
	largeSizeOnDisk += ret;
	return ret;
}

bool JoinPartition::getNextPartition(vector<RGData> *smallData, uint64_t *partitionID, JoinPartition **jp)
{

	if (fileMode) {
		ByteStream bs;
		RGData rgData;

		if (nextPartitionToReturn > 0)
			return false;

		//cout << "reading the small side" << endl;
		nextSmallOffset = 0;
		while (1) {
			readByteStream(0, &bs);
			if (bs.length() == 0)
				break;
			rgData.deserialize(bs);
			//smallRG.setData(&rgData);
			//cout << "read a smallRG with " << smallRG.getRowCount() << " rows" << endl;
			smallData->push_back(rgData);
		}
		nextPartitionToReturn = 1;
		*partitionID = uniqueID;
		*jp = this;
		return true;
	}

	bool ret = false;
	while (!ret && nextPartitionToReturn < bucketCount) {
		ret = buckets[nextPartitionToReturn]->getNextPartition(smallData, partitionID, jp);
		if (!ret)
			nextPartitionToReturn++;
	}
	return ret;
}

boost::shared_ptr<RGData> JoinPartition::getNextLargeRGData()
{
	boost::shared_ptr<RGData> ret;

	ByteStream bs;
	readByteStream(1, &bs);
	if (bs.length() != 0) {
		ret.reset(new RGData());
		ret->deserialize(bs);
	}
	else {
		boost::filesystem::remove(largeFilename);
		largeSizeOnDisk = 0;
	}
	return ret;
}

bool JoinPartition::hasNullJoinColumn(Row &r)
{
	for (uint32_t i = 0; i < smallKeyCols.size(); i++) {
		if (r.isNullValue(smallKeyCols[i]))
			return true;
	}
	return false;
}

void JoinPartition::initForProcessing()
{
	int i;

	nextPartitionToReturn = 0;
	if (!fileMode)
		for (i = 0; i < (int) bucketCount; i++)
			buckets[i]->initForProcessing();
	else
		nextLargeOffset = 0;
}

void JoinPartition::initForLargeSideFeed()
{
	int i;

	if (!rootNode) {
		buffer.reinit(largeRG);
		largeRG.setData(&buffer);
		largeRG.resetRowGroup(0);
		largeRG.getRow(0, &largeRow);
	}

	largeSizeOnDisk = 0;
	if (fileMode)
		nextLargeOffset = 0;
	else
		for (i = 0; i < (int) bucketCount; i++)
			buckets[i]->initForLargeSideFeed();
}

void JoinPartition::saveSmallSidePartition(vector<RGData> &rgData)
{
	//cout << "JP: saving partition: " << id << endl;
	htSizeEstimate = 0;
	smallSizeOnDisk = 0;
	nextSmallOffset = 0;
	boost::filesystem::remove(smallFilename);
	insertSmallSideRGData(rgData);
	doneInsertingSmallData();
}

void JoinPartition::readByteStream(int which, ByteStream *bs)
{
	size_t &offset = (which == 0 ? nextSmallOffset : nextLargeOffset);
	fstream &fs = (which == 0 ? smallFile : largeFile);
	const char *filename = (which == 0 ? smallFilename.c_str() : largeFilename.c_str());

	size_t len;

	bs->restart();

	fs.open(filename, ios::binary | ios::in);
	int saveErrno = errno;
	if (!fs) {
		fs.close();
		ostringstream os;
		os << "Disk join could not open file (read access) " << filename << ": " << strerror(saveErrno) << endl;
		throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
	}

	fs.seekg(offset);
	fs.read((char *) &len, sizeof(len));
	saveErrno = errno;
	if (!fs) {
		if (fs.eof()) {
			fs.close();
			return;
		}
		else {
			fs.close();
			ostringstream os;
			os << "Disk join could not read file " << filename << ": " << strerror(saveErrno) << endl;
			throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
		}
	}
	idbassert(len != 0);
	totalBytesRead += sizeof(len);
	if (!useCompression) {
		bs->needAtLeast(len);
		fs.read((char *) bs->getInputPtr(), len);
		saveErrno = errno;
		if (!fs) {
			fs.close();
			ostringstream os;
			os << "Disk join could not read file " << filename << ": " << strerror(saveErrno) << endl;
			throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
		}
		totalBytesRead += len;
		bs->advanceInputPtr(len);
	}
	else {
		size_t uncompressedSize;
		boost::scoped_array<char> buf(new char[len]);

		fs.read(buf.get(), len);
		saveErrno = errno;
		if (!fs) {
			fs.close();
			ostringstream os;
			os << "Disk join could not read file " << filename << ": " << strerror(saveErrno) << endl;
			throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
		}
		totalBytesRead += len;
		compressor.getUncompressedSize(buf.get(), len, &uncompressedSize);
		bs->needAtLeast(uncompressedSize);
		compressor.uncompress(buf.get(), len, (char *) bs->getInputPtr());
		bs->advanceInputPtr(uncompressedSize);
	}

	offset = fs.tellg();
	fs.close();
}

uint64_t JoinPartition::writeByteStream(int which, ByteStream &bs)
{
	size_t &offset = (which == 0 ? nextSmallOffset : nextLargeOffset);
	fstream &fs = (which == 0 ? smallFile : largeFile);
	const char *filename = (which == 0 ? smallFilename.c_str() : largeFilename.c_str());

	fs.open(filename, ios::binary | ios::out | ios::app);
	int saveErrno = errno;
	if (!fs) {
		fs.close();
		ostringstream os;
		os << "Disk join could not open file (write access) " << filename << ": " << strerror(saveErrno) << endl;
		throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
	}

	uint64_t ret = 0;
	size_t len = bs.length();
	idbassert(len != 0);

	fs.seekp(offset);

	if (!useCompression) {
		ret = len + 4;
		fs.write((char *) &len, sizeof(len));
		fs.write((char *) bs.buf(), len);
		saveErrno = errno;
		if (!fs) {
			fs.close();
			ostringstream os;
			os << "Disk join could not write file " << filename << ": " << strerror(saveErrno) << endl;
			throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
		}
		totalBytesWritten += sizeof(len) + len;
	}
	else {
		uint64_t maxSize = compressor.maxCompressedSize(len);
		size_t actualSize;
		boost::scoped_array<uint8_t> compressed(new uint8_t[maxSize]);

		compressor.compress((char *) bs.buf(), len, (char *) compressed.get(), &actualSize);
		ret = actualSize + 4;
		fs.write((char *) &actualSize, sizeof(actualSize));
		fs.write((char *) compressed.get(), actualSize);
		saveErrno = errno;
		if (!fs) {
			fs.close();
			ostringstream os;
			os << "Disk join could not write file " << filename << ": " << strerror(saveErrno) << endl;
			throw IDBExcept(os.str().c_str(), ERR_DBJ_FILE_IO_ERROR);
		}
		totalBytesWritten += sizeof(actualSize) + actualSize;
	}
	bs.advance(len);

	offset = fs.tellp();
	fs.close();
	return ret;
}

uint64_t JoinPartition::getBytesRead()
{
	uint64_t ret;

	if (fileMode)
		return totalBytesRead;

	ret = totalBytesRead;
	for (int i = 0; i < (int) bucketCount; i++)
		ret += buckets[i]->getBytesRead();
	return ret;
}

uint64_t JoinPartition::getBytesWritten()
{
	uint64_t ret;

	if (fileMode)
		return totalBytesWritten;

	ret = totalBytesWritten;
	for (int i = 0; i < (int) bucketCount; i++)
		ret += buckets[i]->getBytesWritten();
	return ret;
}


} // namespace

