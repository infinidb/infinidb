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

/******************************************************************************************
 * $Id: resourcemanager.h 9491 2013-05-06 20:57:41Z pleblanc $
 *
 ******************************************************************************************/
/**
 * @file
 */
#ifndef JOBLIST_RESOURCEMANAGER_H
#define JOBLIST_RESOURCEMANAGER_H

#include <vector>
#include <iostream>
#include <boost/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <unistd.h>

#include "configcpp.h"
#include "calpontselectexecutionplan.h"
#include "jl_logger.h"
#include "resourcedistributor.h"

#if defined(_MSC_VER) && !defined(_WIN64)
#  ifndef InterlockedAdd
#    define InterlockedAdd64 InterlockedAdd
#    define InterlockedAdd(x, y) ((x) + (y))
#  endif
#endif

#if defined(_MSC_VER) && defined(RESOURCEMANAGER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist 
{
  //aggfilterstep
  const uint32_t  defaultNumThreads = 8;
  //joblistfactory
  const uint32_t defaultFlushInterval = 8 * 1024;
  const uint32_t defaultFifoSize = 10;
  const uint32_t defaultHJFifoSizeLargeSide = 128;
  const uint64_t defaultHJMaxElems = 512 * 1024;   //hashjoin uses 8192
  const int      defaultHJMaxBuckets = 32;	   //hashjoin uses 4
  const uint64_t defaultHJPmMaxMemorySmallSide = 64 * 1024 * 1024;
  const uint64_t defaultHJUmMaxMemorySmallSide = 4 * 1024 * 1024 * 1024ULL;
  const uint32_t defaultTempSaveSize = defaultHJMaxElems;
  const uint64_t defaultTotalUmMemory = 8 * 1024 * 1024 * 1024ULL;
  const uint64_t defaultHUATotalMem = 8 * 1024 * 1024 * 1024ULL;
 
  const uint32_t defaultTupleDLMaxSize = 64 * 1024;
  const uint32_t defaultTupleMaxBuckets = 256;

  //pcolscan.cpp
  const uint32_t defaultScanLbidReqLimit  = 10000;
  const uint32_t defaultScanLbidReqThreshold = 5000;
  const uint32_t defaultLogicalBlocksPerScan = 1024;   // added for bug 1264.
  const uint32_t defaultScanBlockThreshhold = 10000;   //in jobstep.h

  const uint32_t defaultScanReceiveThreads = 8;

  //pcolstep.cpp
  const uint32_t defaultProjectBlockReqLimit  = 32 * 1024;
  const uint32_t defaultProjectBlockReqThreshold = 16 * 1024;  //256 in jobstep.h
  
  //BatchPrimitiveStep
  const uint32_t defaultRequestSize  = 1;
  const uint32_t defaultMaxOutstandingRequests = 20;
  const uint32_t defaultProcessorThreadsPerScan = 16;
  const uint32_t defaultJoinerChunkSize = 16 * 1024 * 1024;

  //bucketreuse
  const std::string defaultTempDiskPath = "/var/tmp";
  const std::string defaultWorkingDir = ".";   //"/tmp";

  //largedatalist
  const uint32_t defaultLDLMaxElements = 32 * 1024 * 1024;

  //zdl
  const uint64_t defaultMaxElementsInMem = 32 * 1024 * 1024;
  const uint64_t defaultNumBuckets = 128;
  const uint64_t defaultMaxElementsPerBuckert = 16 * 1024 * 1024;

  const int defaultEMServerThreads = 50;
  const int defaultEMServerQueueSize = 100;
  const int defaultEMSecondsBetweenMemChecks = 1;
  const int defaultEMMaxPct = 95;
  const int defaultEMPriority = 21; // @Bug 3385
  const int defaultEMExecQueueSize = 20;


  const uint64_t defaultInitialCapacity = 1024 * 1024; 
  const int  defaultTWMaxBuckets = 256;
  const int  defaultPSCount = 0;
  const int  defaultConnectionsPerPrimProc = 1;
  const uint defaultLBID_Shift = 13;
  const uint64_t defaultExtentRows = 8 * 1024 * 1024;

  // DMLProc
  // @bug 1886.  Knocked a 0 off the default below dropping it from 4M down to 256K.  Delete was consuming too much memory.
  const uint64_t defaultDMLMaxDeleteRows  = 256 * 1024; 

  // Connector
  // @bug 2048.  To control batch insert memory usage.
  const uint64_t defaultRowsPerBatch  = 10000; 
  
  /* HJ CP feedback, see bug #1465 */
  const uint defaultHjCPUniqueLimit = 100;

  // Order By and Limit
  const uint64_t defaultOrderByLimitMaxMemory = 1 * 1024 * 1024 * 1024ULL;

  const uint64_t defaultDECThrottleThreshold = 200000000;  // ~200 MB
  

  /** @brief ResourceManager
   *	Returns requested values from Config 
   * 
   */
  class ResourceManager
  {
  public:

    /** @brief ctor
     * 
     */
    EXPORT ResourceManager(bool runningInExeMgr=false);
//     ResourceManager(const config::Config *cf);
//     ResourceManager(const std::string& config);
//passed by ExeMgr and DistributedEngineComm to MessageQueueServer or -Client
    config::Config* getConfig() { return fConfig; }

    /** @brief dtor
     */
    virtual ~ResourceManager() { }

    typedef std::map <uint32_t, uint64_t> MemMap;


    int  	getEmServerThreads() const { return  getUintVal(fExeMgrStr, "ServerThreads", defaultEMServerThreads); }
    int  	getEmServerQueueSize() const { return  getUintVal(fExeMgrStr, "ServerQueueSize", defaultEMServerQueueSize); }
    int  	getEmSecondsBetweenMemChecks() const { return  getUintVal(fExeMgrStr, "SecondsBetweenMemChecks", defaultEMSecondsBetweenMemChecks); }
    int  	getEmMaxPct() const { return  getUintVal(fExeMgrStr, "MaxPct", defaultEMMaxPct); }
    EXPORT int  	getEmPriority() const;
    int  	getEmExecQueueSize() const { return  getUintVal(fExeMgrStr, "ExecQueueSize", defaultEMExecQueueSize); }

    int	      	getHjMaxBuckets() const { return  getUintVal(fHashJoinStr, "MaxBuckets", defaultHJMaxBuckets); }
    unsigned  	getHjNumThreads() const { return  fHjNumThreads; } //getUintVal(fHashJoinStr, "NumThreads", defaultNumThreads); }
    uint64_t  	getHjMaxElems()  const { return  getUintVal(fHashJoinStr, "MaxElems", defaultHJMaxElems); }
    uint32_t  	getHjFifoSizeLargeSide() const { return  getUintVal(fHashJoinStr, "FifoSizeLargeSide", defaultHJFifoSizeLargeSide); }
	uint 		getHjCPUniqueLimit() const { return getUintVal(fHashJoinStr, "CPUniqueLimit", defaultHjCPUniqueLimit); }
	uint64_t	getPMJoinMemLimit() const { return pmJoinMemLimit; }

    uint32_t  	getJLFlushInterval() const { return  getUintVal(fJobListStr, "FlushInterval", defaultFlushInterval); }
    uint32_t  	getJlFifoSize() const { return  getUintVal(fJobListStr, "FifoSize", defaultFifoSize); }
    uint32_t  	getJlScanLbidReqLimit() const { return  getUintVal(fJobListStr, "ScanLbidReqLimit",defaultScanLbidReqLimit); }
    uint32_t  	getJlScanLbidReqThreshold() const { return  getUintVal(fJobListStr,"ScanLbidReqThreshold", defaultScanLbidReqThreshold); }

    // @bug 1264 - Added LogicalBlocksPerScan configurable which determines the number of blocks contained in each BPS scan request.
    uint32_t    getJlLogicalBlocksPerScan() const { return  getUintVal(fJobListStr,"LogicalBlocksPerScan", defaultLogicalBlocksPerScan); }  
    uint32_t  	getJlProjectBlockReqLimit() const { return  getUintVal(fJobListStr, "ProjectBlockReqLimit", defaultProjectBlockReqLimit  ); }
    uint32_t  	getJlProjectBlockReqThreshold() const { return  getUintVal(fJobListStr,"ProjectBlockReqThreshold", defaultProjectBlockReqThreshold);}
    uint32_t  	getJlNumScanReceiveThreads() const { return  fJlNumScanReceiveThreads; } //getUintVal(fJobListStr, "NumScanReceiveThreads", defaultScanReceiveThreads); }
    
    // @bug 1424,1298
    uint32_t    getJlProcessorThreadsPerScan() const { return  fJlProcessorThreadsPerScan; } //getUintVal(fJobListStr,"ProcessorThreadsPerScan", defaultProcessorThreadsPerScan); }  
    uint32_t  	getJlRequestSize() const { return  getUintVal(fJobListStr, "RequestSize", defaultRequestSize  ); }
    uint32_t  	getJlMaxOutstandingRequests() const { return  getUintVal(fJobListStr,"MaxOutstandingRequests", defaultMaxOutstandingRequests);}
    uint32_t  	getJlJoinerChunkSize() const { return  getUintVal(fJobListStr,"JoinerChunkSize", defaultJoinerChunkSize);}

    int	      	getPsCount() const { return  getUintVal(fPrimitiveServersStr, "Count", defaultPSCount ); }
    int	      	getPsConnectionsPerPrimProc() const { return getUintVal(fPrimitiveServersStr, "ConnectionsPerPrimProc", defaultConnectionsPerPrimProc); } 
    uint      	getPsLBID_Shift() const { return  getUintVal(fPrimitiveServersStr, "LBID_Shift", defaultLBID_Shift ); }
    bool      	getPsMulticast() const 
    {  
      std::string val(getStringVal(fPrimitiveServersStr, "Multicast", "N" ));  
	boost::to_upper(val); 
	return "Y" == val; 
    }
    bool      	getPsMulticastLoop() const 
    {  
      std::string val(getStringVal(fPrimitiveServersStr, "MulticastLoop", "N" ));  
	boost::to_upper(val); 
	return "Y" == val; 
    }

    std::string getScTempDiskPath() const { return  getStringVal(fSystemConfigStr, "TempDiskPath", defaultTempDiskPath  ); }
    uint64_t  	getScTempSaveSize() const { return  getUintVal(fSystemConfigStr, "TempSaveSize", defaultTempSaveSize); }
    std::string getScWorkingDir() const { return  getStringVal(fSystemConfigStr, "WorkingDir", defaultWorkingDir ); }

    uint32_t  	getTwMaxSize() const { return  getUintVal(fTupleWSDLStr, "MaxSize",defaultTupleDLMaxSize  ); }
    uint64_t  	getTwInitialCapacity() const { return  getUintVal(fTupleWSDLStr, "InitialCapacity", defaultInitialCapacity  ); } 
    int       	getTwMaxBuckets	() const { return  getUintVal(fTupleWSDLStr, "MaxBuckets", defaultTWMaxBuckets  ); }
    uint8_t   	getTwNumThreads() const { return  fTwNumThreads; } //getUintVal(fTupleWSDLStr, "NumThreads", defaultNumThreads  ); }
    uint64_t  	getZdl_MaxElementsInMem() const { return  getUintVal(fZDLStr,"ZDL_MaxElementsInMem", defaultMaxElementsInMem  ); }
    uint64_t  	getZdl_MaxElementsPerBucket () const { return  getUintVal(fZDLStr, "ZDL_MaxElementsPerBucket", defaultMaxElementsPerBuckert ); }

    uint64_t  	getExtentRows() const { return  getUintVal(fExtentMapStr, "ExtentRows", defaultExtentRows  ); }

    uint32_t 	getDBRootCount() const { return getUintVal(fSystemConfigStr, "DBRootCount", 1); }
    uint32_t 	getPMCount() const { return getUintVal(fPrimitiveServersStr, "Count", 1); }

    std::vector<std::string>	getHbrPredicate() const
      {
	std::vector<std::string> columns;
	fConfig->getConfig(fHashBucketReuseStr, "Predicate", columns);
	return columns;
      }

    uint64_t  	getDMLMaxDeleteRows () const
	{ return  getUintVal(fDMLProcStr, "MaxDeleteRows", defaultDMLMaxDeleteRows); }

    uint64_t  	getRowsPerBatch() const
	{ return  getUintVal(fBatchInsertStr, "RowsPerBatch", defaultRowsPerBatch); }

    uint64_t  	getOrderByLimitMaxMemory() const
	{ return  getUintVal(fOrderByLimitStr, "MaxMemory", defaultOrderByLimitMaxMemory); }

	uint64_t	getDECThrottleThreshold() const
	{ return getUintVal(fJobListStr, "DECThrottleThreshold", defaultDECThrottleThreshold); }

    EXPORT void  emServerThreads();
    EXPORT void  emServerQueueSize();
    EXPORT void  emSecondsBetweenMemChecks();
    EXPORT void  emMaxPct();
    EXPORT void  emPriority();
    EXPORT void  emExecQueueSize();

    EXPORT void  hjNumThreads();
    EXPORT void  hjMaxBuckets();
    EXPORT void  hjMaxElems();
    EXPORT void  hjFifoSizeLargeSide();
    EXPORT void  hjPmMaxMemorySmallSide();

	/* new HJ/Union/Aggregation mem interface, used by TupleBPS */
	inline bool getMemory(int64_t amount) {
#ifdef _MSC_VER
		return InterlockedAdd64(&totalUmMemLimit, -amount) >= 0;
#else
		return __sync_sub_and_fetch(&totalUmMemLimit, amount) >= 0;
#endif
	}
	inline void returnMemory(int64_t amount) {
#ifdef _MSC_VER
		InterlockedAdd64(&totalUmMemLimit, amount);
#else
		__sync_add_and_fetch(&totalUmMemLimit, amount);
#endif
	}
	inline int64_t availableMemory() { return totalUmMemLimit; }

	/* old HJ mem interface, used by HashJoin */
	uint64_t   getHjPmMaxMemorySmallSide(uint32_t sessionID)
	  { return fHJPmMaxMemorySmallSideSessionMap.getSessionResource(sessionID); }
	uint64_t   getHjUmMaxMemorySmallSide(uint32_t sessionID)
	  { return fHJUmMaxMemorySmallSideDistributor.getSessionResource(sessionID);       }
	uint64_t   getHjTotalUmMaxMemorySmallSide() const
	  { return fHJUmMaxMemorySmallSideDistributor.getTotalResource(); }

	EXPORT void addHJUmMaxSmallSideMap(uint32_t sessionID, uint64_t mem);

	void removeHJUmMaxSmallSideMap(uint32_t sessionID) { fHJUmMaxMemorySmallSideDistributor.removeSession(sessionID); }

	EXPORT void addHJPmMaxSmallSideMap(uint32_t sessionID, uint64_t mem);
	void removeHJPmMaxSmallSideMap(uint32_t sessionID) { fHJPmMaxMemorySmallSideSessionMap.removeSession(sessionID); }

	void removeSessionMaps(uint32_t sessionID)
	{
		fHJPmMaxMemorySmallSideSessionMap.removeSession(sessionID);
		fHJUmMaxMemorySmallSideDistributor.removeSession(sessionID);
	}
 
	uint64_t requestHJMaxMemorySmallSide(uint32_t sessionID, uint64_t amount) {
	return fHJUmMaxMemorySmallSideDistributor.requestResource(sessionID, amount); }

	uint64_t requestHJUmMaxMemorySmallSide(uint32_t sessionID) { return fHJUmMaxMemorySmallSideDistributor.requestResource(sessionID); }
	void returnHJUmMaxMemorySmallSide(uint64_t mem) {
		fHJUmMaxMemorySmallSideDistributor.returnResource(mem); }

    EXPORT void  jlFlushInterval();
    EXPORT void  jlFifoSize();
    EXPORT void  jlScanLbidReqLimit();
    EXPORT void  jlScanLbidReqThreshold();
    EXPORT void  jlProjectBlockReqLimit();
    EXPORT void  jlProjectBlockReqThreshold();
    EXPORT void  jlNumScanReceiveThreads();

    EXPORT void  psCount();
    EXPORT void  psConnectionsPerPrimProc() ;
    EXPORT void  psLBID_Shift(); 

    EXPORT void  scTempDiskPath();
    EXPORT void  scTempSaveSize() ;
    EXPORT void  scWorkingDir();

    EXPORT void  twMaxSize();
    EXPORT void  twInitialCapacity() ;
    EXPORT void  twMaxBuckets	() ;
    EXPORT void  twNumThreads();

    EXPORT void  zdl_MaxElementsInMem();
    EXPORT void  zdl_MaxElementsPerBucket() ;

    EXPORT void  hbrPredicate();

    void setTraceFlags(uint32_t flags) 
    { 
		fTraceFlags = flags; 
		fHJUmMaxMemorySmallSideDistributor.setTrace(((fTraceFlags & execplan::CalpontSelectExecutionPlan::TRACE_RESRCMGR) != 0));
    }
    bool rmtraceOn() const { return ((fTraceFlags & execplan::CalpontSelectExecutionPlan::TRACE_RESRCMGR) != 0); }

    void numCores(unsigned numCores) { fNumCores = numCores; }
    unsigned numCores() const { return fNumCores; }
    
    void aggNumThreads(uint numThreads) { fAggNumThreads = numThreads; }
    uint aggNumThreads() const { return fAggNumThreads; }
    
    void aggNumBuckets(uint numBuckets) { fAggNumBuckets = numBuckets; }
    uint aggNumBuckets() const { return fAggNumBuckets; }
    
    void aggNumRowGroups(uint numRowGroups) { fAggNumRowGroups = numRowGroups; }
    uint aggNumRowGroups() const { return fAggNumRowGroups; }

	EXPORT bool getMysqldInfo(std::string& h, std::string& u, std::string& w, unsigned int& p) const;
	EXPORT bool queryStatsEnabled() const;
	EXPORT bool userPriorityEnabled() const;

  private:

    void logResourceChangeMessage(logging::LOG_TYPE logType, uint32_t sessionID, uint64_t newvalue, uint64_t value, const std::string& source, logging::Message::MessageID mid);
    /** @brief get name's value from section
     *
     * get name's value from section in the current config file or default value .
     * @param section the name of the config file section to search
     * @param name the param name whose value is to be returned
     * @param defVal the default value returned if the value is missing
     */
    std::string getStringVal(const std::string& section, const std::string& name, const std::string& defVal) const;

    template<typename IntType>
      IntType getUintVal(const std::string& section, const std::string& name, IntType defval) const;

    template<typename IntType>
      IntType getIntVal(const std::string& section, const std::string& name, IntType defval) const;

    void logMessage(logging::LOG_TYPE logLevel, logging::Message::MessageID mid, uint64_t value = 0, uint32_t sessionId = 0);

    /*static	const*/ std::string fExeMgrStr;
    static	const std::string fHashJoinStr;
    static	const std::string fHashBucketReuseStr;
    static	const std::string fJobListStr;
    static	const std::string fPrimitiveServersStr;
    /*static	const*/ std::string fSystemConfigStr;
    static	const std::string fTupleWSDLStr;
    static	const std::string fZDLStr;
    static	const std::string fExtentMapStr;
    /*static	const*/ std::string fDMLProcStr;
	/*static	const*/ std::string fBatchInsertStr;
	static	const std::string fOrderByLimitStr;
    config::Config* fConfig;

    uint32_t 		fTraceFlags;

    unsigned fNumCores;
    unsigned fHjNumThreads;
    uint32_t fJlProcessorThreadsPerScan;
    uint32_t fJlNumScanReceiveThreads;
    uint8_t fTwNumThreads;

	/* old HJ support */
	ResourceDistributor	fHJUmMaxMemorySmallSideDistributor;
	LockedSessionMap   fHJPmMaxMemorySmallSideSessionMap;

	/* new HJ/Union/Aggregation support */
#ifdef _MSC_VER
	volatile LONGLONG totalUmMemLimit;
#else
	int64_t totalUmMemLimit;	// mem limit for join, union, and aggregation on the UM
#endif
	uint64_t pmJoinMemLimit;	// mem limit on individual PM joins

	/* multi-thread aggregate */
	uint fAggNumThreads;
	uint fAggNumBuckets;
	uint fAggNumRowGroups;

	bool isExeMgr;
  };


  inline std::string ResourceManager::getStringVal(const std::string& section, const std::string& name, const std::string& defval) const
    {
      std::string val = fConfig->getConfig(section, name);
#ifdef DEBUGRM
	std::cout << "RM getStringVal for " << section << " : " << name << " val: " << val << " default: " << defval << std::endl;
#endif
      return (0 == val.length() ? defval : val);
    }

  template<typename IntType> 
    inline IntType ResourceManager::getUintVal(const std::string& section, const std::string& name, IntType defval) const
    {
      IntType val = fConfig->uFromText(fConfig->getConfig(section, name));
#ifdef DEBUGRM
	std::cout << "RM getUintVal val: " << section << " : " << name << " val: " << val << " default: " << defval << std::endl;
#endif
      return ( 0 == val ? defval : val );

    }

  template<typename IntType> 
    inline IntType ResourceManager::getIntVal(const std::string& section, const std::string& name, IntType defval) const
    {
      std::string retStr = fConfig->getConfig(section, name);
#ifdef DEBUGRM
	std::cout << "RM getIntVal val: " << section << " : " << name << " val: " << retStr << " default: " << defval << std::endl;
#endif
      return ( 0 == retStr.length() ? defval : fConfig->fromText(retStr) );
    }



}

#undef EXPORT

#endif
