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


#ifndef JOINPARTITION_H
#define JOINPARTITION_H

#include "rowgroup.h"
#include "hasher.h"
#include "idbcompress.h"
#include <vector>
#include <fstream>
#include <boost/thread.hpp>

namespace joiner {

class JoinPartition
{
	public:
		JoinPartition();
		JoinPartition(const rowgroup::RowGroup &largeRG,
						const rowgroup::RowGroup &smallRG,
						const std::vector<uint32_t> &smallkeyCols,
						const std::vector<uint32_t> &largeKeyCols,
						bool typeless,
						bool isAntiWithMatchNulls,
						bool hasFEFilter,
						uint64_t totalUMMemory,
						uint64_t partitionSize);
		JoinPartition(const JoinPartition &, bool splitMode);

		virtual ~JoinPartition();

		// For now, the root node will use the RGData interface, the branches & leaves use
		// only the Row interface.
		int64_t insertSmallSideRow(const rowgroup::Row &row);
		int64_t insertSmallSideRGData(rowgroup::RGData &);
		// note, the vector version of this fcn frees the input RGDatas as it goes
		int64_t insertSmallSideRGData(std::vector<rowgroup::RGData> &);
		int64_t doneInsertingSmallData();
		int64_t insertLargeSideRGData(rowgroup::RGData &);
		int64_t insertLargeSideRow(const rowgroup::Row &row);
		int64_t doneInsertingLargeData();

		/* Returns true if there are more partitions to fetch, false otherwise */
		bool getNextPartition(std::vector<rowgroup::RGData> *smallData, uint64_t *partitionID,
								JoinPartition **jp);

		boost::shared_ptr<rowgroup::RGData> getNextLargeRGData();

		/* It's important to follow the sequence of operations to maintain the correct
		   internal state.  Right now it doesn't check that you the programmer are doing things
		   right, it'll likely fail queries or crash if you do things wrong.
		   This should be made simpler at some point.

		   On construction, the JP is config'd for small-side reading.
		   After that's done, call doneInsertingSmallData() and initForLargeSideFeed().
		   Then, insert the large-side data.  When done, call doneInsertingLargeData()
		   and initForProcessing().
		   In the processing phase, use getNextPartition() and getNextLargeRGData()
		   to get the data back out.  After processing all partitions, if it's necessary
		   to process more iterations of the large side, call initForProcessing() again, and
		   continue as before.
		*/



		/* Call this before reading into the large side */
		void initForLargeSideFeed();
		/* Call this between large-side insertion & join processing */
		void initForProcessing();
		/* Small outer joins need to retain some state after each large-side iteration */
		void saveSmallSidePartition(std::vector<rowgroup::RGData> &rgdata);

		/* each JP instance stores the sizes of every JP instance below it, so root node has the total. */
		int64_t getCurrentDiskUsage() { return smallSizeOnDisk + largeSizeOnDisk; }
		int64_t getSmallSideDiskUsage() { return smallSizeOnDisk; }
		int64_t getLargeSideDiskUsage() { return largeSizeOnDisk; }

		uint64_t getBytesRead();
		uint64_t getBytesWritten();
		uint64_t getMaxLargeSize() { return maxLargeSize; }
		uint64_t getMaxSmallSize() { return maxSmallSize; }

	protected:
	private:
		void initBuffers();
		int64_t convertToSplitMode();
		int64_t processSmallBuffer();
		int64_t processLargeBuffer();

		int64_t processSmallBuffer(rowgroup::RGData &);
		int64_t processLargeBuffer(rowgroup::RGData &);

		rowgroup::RowGroup smallRG;
		rowgroup::RowGroup largeRG;
		std::vector<uint32_t> smallKeyCols;
		std::vector<uint32_t> largeKeyCols;
		bool typelessJoin;
		uint32_t hashSeed;
		std::vector<boost::shared_ptr<JoinPartition> > buckets;
		uint32_t bucketCount;   //  = TotalUMMem / htTargetSize

		bool fileMode;
		std::fstream smallFile;
		std::fstream largeFile;
		std::string filenamePrefix;
		std::string smallFilename;
		std::string largeFilename;
		rowgroup::RGData buffer;
		rowgroup::Row smallRow;
		rowgroup::Row largeRow;
		uint32_t nextPartitionToReturn;
		uint64_t htSizeEstimate;
		uint64_t htTargetSize;
		uint64_t uniqueID;
		uint64_t smallSizeOnDisk;
		uint64_t largeSizeOnDisk;
		utils::Hasher_r hasher;
		bool rootNode;

		/* Not-in antijoin hack.  A small-side row with a null join column has to go into every partition or
		into one always resident partition (TBD).

		If an F&E filter exists, it needs all null rows, if not, it only needs one. */
		bool antiWithMatchNulls;
		bool needsAllNullRows;
		bool gotNullRow;
		bool hasNullJoinColumn(rowgroup::Row &);

		// which = 0 -> smallFile, which = 1 -> largeFile
		void readByteStream(int which, messageqcpp::ByteStream *bs);
		uint64_t writeByteStream(int which, messageqcpp::ByteStream &bs);

		/* Compression support */
		bool useCompression;
		compress::IDBCompressInterface compressor;
		/* TBD: do the reading/writing in one thread, compression/decompression in another */

		/* Some stats for reporting */
		uint64_t totalBytesRead, totalBytesWritten;
		uint64_t maxLargeSize, maxSmallSize;

		/* file descriptor reduction */
		size_t nextSmallOffset;
		size_t nextLargeOffset;
};



}

#endif // JOINPARTITION_H
