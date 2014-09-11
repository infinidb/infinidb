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

//
// $Id: hashjoin.h 7406 2011-02-08 02:05:10Z zzhu $
// C++ Interface: hashjoin
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef HASHJOIN_H_
#define HASHJOIN_H_

#include "jobstep.h"
#include "calpontsystemcatalog.h"
#include <joiner.h>
#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <vector>
#include <fstream>
#include <utility>

namespace joblist
{

class HashJoinStep : public JobStep
{
	public:
		HashJoinStep(JoinType, uint32_t sessionId, uint32_t statementId,
			uint32_t txnId, ResourceManager *);
		virtual ~HashJoinStep();

		void hjRunner();

		void setLargeSideBPS(BatchPrimitive*);
		void setSmallSideCardEst(uint64_t card);
		void setLargeSideStepsOut(const std::vector<SJSTEP>& largeSideSteps);
		void setSmallSideStepsOut(const std::vector<SJSTEP>& smallSideSteps);
		void setSizeFlag(bool);   // true -> left is smaller, false ->right is smaller
		void setAllowLargeSideFifoToDisk(bool);// how handle blocked fifo

		/* mandatory JobStep interface */
		void run();
		void abort();
		void join();
		const JobStepAssociation& inputAssociation() const;
		void inputAssociation(const JobStepAssociation& inputAssociation);
		const JobStepAssociation& outputAssociation() const;
		void outputAssociation(const JobStepAssociation& outputAssociation);
		const std::string toString() const;
		void stepId(uint16_t stepId);
		uint16_t stepId() const;
		uint32_t sessionId() const;
		uint32_t txnId() const;
		uint32_t statementId() const;
		void logger(const SPJL& logger);

		execplan::CalpontSystemCatalog::OID tableOid() const{return fTableOID2;}
		execplan::CalpontSystemCatalog::OID tableOid1()const{return fTableOID1;}
		execplan::CalpontSystemCatalog::OID tableOid2()const{return fTableOID2;}
		void tableOid1(execplan::CalpontSystemCatalog::OID tableOid1)
			{ fTableOID1 = tableOid1; }
		void tableOid2(execplan::CalpontSystemCatalog::OID tableOid2)
			{ fTableOID2 = tableOid2; }

		std::string alias1() const { return fAlias1; }
		void alias1(const std::string& alias) { fAlias1 = alias; }
		std::string alias2() const { return fAlias2; }
		void alias2(const std::string& alias) { fAlias = fAlias2 = alias; }

		std::string view1() const { return fView1; }
		void view1(const std::string& vw) { fView1 = vw; }
		std::string view2() const { return fView2; }
		void view2(const std::string& vw) { fView = fView2 = vw; }

		std::map<execplan::CalpontSystemCatalog::OID, uint64_t>& tbCardMap()
			{ return fTbCardMap; }

		enum OutputMode {
			BOTH,
			LEFTONLY,
			RIGHTONLY
		};
		void setOutputMode(OutputMode);

		enum HashJoinMode {
			PM_MEMORY,
			UM_MEMORY,
			LARGE_HASHJOIN_CARD,
			LARGE_HASHJOIN_RUNTIME
		};
		HashJoinMode hashJoinMode() const { return fHashJoinMode; }
		bool didOutputAssocUseDisk(unsigned int index) const;

		/* Store BucketReuse info in case it can be used during LHJ */
		void addBucketReuse(FifoDataList*                 pFifoDL,
			const std::string&                            filterString,
			execplan::CalpontSystemCatalog::OID           tblOid,
			execplan::CalpontSystemCatalog::OID           colOid,
			uint32_t                                      verId,
			execplan::CalpontSystemCatalog::TableColName& colName);
		double getUmMemoryTime() { return fElapsedTimes[WAIT_FOR_UMMEMORY].second + fElapsedTimes[RETURN_UNUSED_MEMORY].second + fElapsedTimes[RETURN_USED_MEMORY].second; }

	private:
		HashJoinStep();
		HashJoinStep(const HashJoinStep &);
		HashJoinStep & operator=(const HashJoinStep &);

		void errorLogging(const std::string& msg) const;
		void startAdjoiningSteps();
		bool outputLargeSide(FifoDataList *largeDL,
			uint          largeIt,
			bool          sendLarge,
			bool          sendSmall,
			FifoDataList *largeOut,
			DataList_t   *largeOutDL,
			uint64_t     *largeCount);
		void outputSmallSide(bool sendSmall,
			bool sendAllSmallSide,
			FifoDataList *smallOut,
			DataList_t   *smallOutDL,
			uint64_t     *smallCount);

		/* large hash join fcns */
		void performLargeHashJoin(
			FifoDataList* smallDL,
			uint smallIt);
		void buildLhjInJsa(
			const  AnyDataListSPtr& anyDl1,
			JobStep*                jobStep2,
			JobStepAssociation&     newJsa1);
		void buildLhjOutJsa(
			const  AnyDataListSPtr& anyDl1,
			std::vector<SJSTEP>&    jobSteps2,
			JobStepAssociation&     newJsa1,
			bool                    dropInserts);
		void printJSA(
			const char* label,
			const JobStepAssociation& jsa);
		void initiateBucketReuse( unsigned int idx );

		/* functions used when we have to cache large-side FIFO to disk */
		bool checkIfFifoBlocked(FifoDataList *largeOut);
		void outputLargeSideFifoToDisk(
			FifoDataList *largeDL,
			uint          largeIt,
			uint64_t     *largeCount,
			FifoDataList *largeOut);
		void writeRowGroupToDisk(const UintRowGroup &rg);
		void createTempFile();
		std::string getFilename();
		void outputLargeSideFifoFromDisk(FifoDataList *largeOut);
		bool readRowGroupFromDisk(UintRowGroup *rg);

		/* enum and functions to track elapsed times */
		enum HjElapsedTime {
			READ_SMALL_SIDE           = 0, // time to read small-side
			WRITE_SMALL_SIDE          = 1, // time to write small-side
			WAIT_FOR_LARGE_SIDE       = 2, // time waiting for 1st L-side read
			RW_LARGE_SIDE_DL          = 3, // time to read/write L-side datalist
			LARGE_SIDE_FIFO_TO_DISK   = 4, // time to save L-side fifo to disk
			LARGE_SIDE_FIFO_FROM_DISK = 5, // time to get L-side fifo from disk
			START_OTHER_STEPS         = 6, // time to startup adjacent jobsteps
			WAIT_FOR_UMMEMORY         = 7, // time waiting for ummemory from Resource Manager
			RETURN_UNUSED_MEMORY      = 8, // time to return unused memory to Resource Manager
			RETURN_USED_MEMORY        = 9, // time to return used memory to Resource Manager
			NUM_ELAPSED_TIMES              // last enumeration value + 1
		};
		void startElapsedTime(HjElapsedTime whichTimeToStart);
		void stopElapsedTime (HjElapsedTime whichTimeToStop);

		/* data members... */
		JobStepAssociation inJSA;
		JobStepAssociation outJSA;

		JoinType joinType;
		uint32_t sessionID;
		uint32_t stepID;
		uint32_t statementID;
		uint32_t txnID;
		ResourceManager *resourceManager;
		bool sizeFlag;
		bool fAllowLargeSideFifoToDisk;
		OutputMode om;
		uint64_t leftCount, rightCount;
		SPJL fLogger;

		boost::shared_ptr<joiner::Joiner> joiner;
		BatchPrimitive* bps;
		uint64_t            fSmallCardinalityEstimate;
		std::vector<SJSTEP> fSmallSideStepsOut;
		std::vector<SJSTEP> fLargeSideStepsOut;
		uint64_t        fUMThreshold, fPMThreshold;
		LargeHashJoin*  fLhjStep;
		HashJoinMode    fHashJoinMode;

		struct HJRunner {
			HJRunner(HashJoinStep *hj) : HJ(hj) { }
			void operator()() { HJ->hjRunner(); }
			HashJoinStep *HJ;
		};
		boost::shared_ptr<boost::thread> mainRunner;

		struct HJSmallSideWriter {
			HJSmallSideWriter(HashJoinStep* hj, bool sendSmall,
				bool sendAllSmallSide,
				FifoDataList *smallOut,
				DataList_t   *smallOutDL,
				uint64_t     *smallCount) : HJ(hj),
					fSendSmall(sendSmall),
					fSendAllSmallSide(sendAllSmallSide),
					fSmallOut(smallOut),
					fSmallOutDL(smallOutDL),
					fSmallCount(smallCount) { }
			void operator()() { HJ->outputSmallSide( fSendSmall, fSendAllSmallSide,
				fSmallOut, fSmallOutDL, fSmallCount ); }
			HashJoinStep *HJ;
			bool          fSendSmall;
			bool          fSendAllSmallSide;
			FifoDataList *fSmallOut;
			DataList_t   *fSmallOutDL;
			uint64_t     *fSmallCount;
		};
		boost::shared_ptr<boost::thread> smallSideWriter;

		execplan::CalpontSystemCatalog::OID fTableOID1;
		execplan::CalpontSystemCatalog::OID fTableOID2;

		std::string fAlias1;
		std::string fAlias2;

		std::string fView1;
		std::string fView2;

		std::map<execplan::CalpontSystemCatalog::OID, uint64_t> fTbCardMap;

		/* tracks elapsed times.                                      */
		/* ".first" is current timer start, ".second" is elapsed time */
		std::pair<double,double> fElapsedTimes[NUM_ELAPSED_TIMES];

		/* data members relating to FIFO caching to temp disk */
		enum CompressionMode    // denotes how data is stored in FIFO temp disk
		{
			COMPRESS_NONE  = 1, // no compression, leave as 64bitRID , 64bitVal
			COMPRESS_64_32 = 2, // compress to 64bit RID, 32 bit value
			COMPRESS_32_64 = 3, // compress to 32bit RID, 64 bit value
			COMPRESS_32_32 = 4, // compress to 32bit RID, 32 bit value
			COMPRESS_64    = 5, // compress to 64bit RID only
			COMPRESS_32    = 6  // compress to 32bit RID only
		};
		std::string  fTempFilePath;    // dir path of temp file
		std::string  fFilename;        // filename of temp file
		std::fstream fFile;            // fstream used to write temp file
		uint64_t     fFilenameCounter; // counter used in temp file name
		CompressionMode         fCompMode;  // how data is stored on temp disk
		boost::shared_ptr<char> fCompBuffer;// temp buffer used for compression

		/* data members relating to BucketReuse; only applicable to LHJ */
		struct BucketReuseParms
		{
			BucketReuseParms() : fBucketReuseEntry(0),
				fReadOnly(false),
				fStep(0),
				fStepUsing(false),
				fColOid(0),
				fTblOid(0),
				fVerId(0) { }
			BucketReuseControlEntry* fBucketReuseEntry;
			bool                     fReadOnly;
			BucketReuseStep*         fStep;
			bool                     fStepUsing;
			execplan::CalpontSystemCatalog::OID fColOid;
			execplan::CalpontSystemCatalog::OID fTblOid;
			uint32_t                 fVerId;
			execplan::CalpontSystemCatalog::TableColName fColName;
			std::string              fFilterString;
		};
		BucketReuseParms fBucketReuseParms[2];
		uint64_t fumMemory;
		uint64_t fpmMemory;
		uint64_t fhjMemory;

};

}

#endif
