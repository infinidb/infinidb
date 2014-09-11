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

/*****************************************************************************
 * $Id: aggregatefilterstep.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 ****************************************************************************/

#include <time.h>
#include <sstream>
#include <set>
#include <typeinfo>
#include <boost/thread.hpp>
using namespace std;

#include "jobstep.h"
#include "datalist.h"
#include "calpontsystemcatalog.h"
#include "simplecolumn.h"
#include "aggregator.h"
using namespace execplan;

#include <boost/algorithm/string/case_conv.hpp>

/** Debug macro */
#define AF_DEBUG 0
#if AF_DEBUG
#define AFDEBUG std::cout
#else
#define AFDEBUG if (false) std::cout
#endif

namespace joblist {
const unsigned DEFAULT_NUM_THREADS = 4;
const uint64_t INSERT_VEC_SIZE = 4096;

struct AFRunner {
    AggregateFilterStep *step;
    
    AFRunner(AggregateFilterStep *s) : step(s)
    { }
    
    void operator()() {
        try {
            step->doAggFilter();
        }
        catch (exception &e) {
            cout << "AggregateFilterStep: doAggFilter caught: " << e.what() << endl;
	    catchHandler(e.what());
//             throw;  
        }
        catch (...) {
	    string msg("AggregateFilterStep doAggFilter caught something not an exception!");
            cout << msg << endl;
	    catchHandler(msg);
//             throw;  
        }
    }
};

template <typename result_t>
class ThreadedAggFilter
{
public:
	typedef vector<AggregateFilterStep::TFilter<result_t> > FilterVec;

	ThreadedAggFilter(AggregateFilterStep *ag, uint8_t threadID, FilterVec& f) : 
	fAggFilter(ag), fThreadID(threadID), fFilters(f) { }

	void operator()()
	{
		try {
			fAggFilter->doThreadedAggFilter<result_t>(fThreadID, fFilters);
		}
		catch (exception &e) {
			cout << "AggregateFilterStep: doThreadedAggFilter caught: " << e.what() << endl;
			catchHandler(e.what());
		}
		catch (...) {
			string msg("AggregateFilterStep doThreadedAggFilter caught something not an exception!");
			cout << msg << endl;
			catchHandler(msg);
		}
	}

private:
	AggregateFilterStep *fAggFilter;
	uint8_t fThreadID;       
	FilterVec fFilters;
};

AggregateFilterStep::AggregateFilterStep(const JobStepAssociation &in, 
        const JobStepAssociation &out,
        const std::string& functionName,
        const execplan::CalpontSelectExecutionPlan::ReturnedColumnList groupByCols,
        execplan::CalpontSelectExecutionPlan::ReturnedColumnList aggCols,
        execplan::SRCP aggParam,	    
        execplan::CalpontSystemCatalog::OID tableOID,
        uint32_t sessionID,
        uint32_t txnId,
        uint32_t verId,
        uint16_t stepID,
        uint32_t statementID,
	ResourceManager& rm ) : JobStep(),
                                fInJSA(in),
                                fOutJSA(out),
                                fFunctionName(functionName),
                                fGroupByCols(groupByCols),
                                fAggCols(aggCols),
                                fAggParam(aggParam),
                                fTableOID(tableOID),
                                fSessionID(sessionID),
                                fTxnID(txnId),
                                fVerID(verId),
                                fStepID(stepID),
                                fStatementID(statementID),
                                fHashLen(0),
                                fDataLen(0),
				fRm(rm),
				fFifoRowCount(0)
{
    fCsc = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);   
    assert(fInJSA.outSize() == 1);
    assert(fOutJSA.outSize() == 1);
    initialize();
}

AggregateFilterStep::~AggregateFilterStep()
{
    //pthread_mutex_destroy(&fFifoMutex);
}

void AggregateFilterStep::join()
{
    runner->join();
}

const string AggregateFilterStep::toString() const
{
    DataList_t* dl = fOutJSA.outAt(0)->dataList();
    ostringstream oss;
    oss << "AggregateFilterStep      ses: " << fSessionID << " txn:" << fTxnID << " ver:" << fVerID;
    oss << " st:" << stepId() << " tb:" << fTableOID;
    oss << " (0x" << hex << (ptrdiff_t)dl << dec << "[" << AnyDataList::dlType(dl) << "])";
    oss << " in:";
    for (unsigned i = 0; i < fInJSA.outSize(); i++)
    {
        oss << fInJSA.outAt(i);
    }
    oss << "Function Name: " << fFunctionName << endl;
    oss << "Group by Cols: " << *(fGroupByCols[0]) << endl;
    oss << "Project cols: " << *(fAggCols[0]) << endl; 
    
    return oss.str();
}

void AggregateFilterStep::outputCol(const execplan::CalpontSystemCatalog::OID& outputCol)
{
    fOutputCol = outputCol;
    MetaDataVec::iterator it;
    for (it = fGroupByMetas.begin(); it != fGroupByMetas.end(); it++)
    {
        if (it->colType.columnOID == fOutputCol)
            fOutputMeta = (*it);
    }
    // only support output column is on group by list
    assert (fOutputMeta.startPos < fHashLen);
}

void AggregateFilterStep::run()
{
    if (traceOn())
    {
        syslogStartStep(16,         
            string("AggregateFilterStep")); 	
    }
    runner.reset(new boost::thread(AFRunner(this)));
}

void AggregateFilterStep::initialize()
{
    // set hashLen, dataLen and metadata for aggCols, outputCol and aggParam
    CalpontSelectExecutionPlan::ReturnedColumnList::const_iterator gbIt;
    CalpontSelectExecutionPlan::ReturnedColumnList::const_iterator aggIt;
    
    SRCP srcp;
    MetaData md;
    SimpleColumn* sc;
      
    for (gbIt = fGroupByCols.begin(); gbIt != fGroupByCols.end(); gbIt++)
    {
        sc = dynamic_cast<SimpleColumn*>(gbIt->get());
        if (sc == 0)
            throw logic_error ("non-simple group by column is not supported.");
        md.colType = fCsc->colType(sc->oid());
        roundColType(md.colType);
              
        md.startPos = fHashLen;
        fGroupByMetas.push_back(md);
        fHashLen += md.colType.colWidth;
    }
    
    for (aggIt = fAggCols.begin(); aggIt != fAggCols.end(); aggIt++)
    {
        sc = dynamic_cast<SimpleColumn*>(aggIt->get());
        if (sc == 0)
            throw logic_error ("aggregate projected columns should be simple columns");
        md.colType = fCsc->colType(sc->oid());
        roundColType(md.colType);
        md.startPos = fHashLen + fDataLen;
        fAggMetas.push_back(md);
        fDataLen += md.colType.colWidth;
    }  
    
    // set fAggOp according to functionName
    boost::algorithm::to_upper(fFunctionName);
    if (fFunctionName == "SUM")
        fOp = Aggregator::SUM;
    else if (fFunctionName == "COUNT")
        fOp = Aggregator::COUNT;
    else if (fFunctionName == "AVG")
        fOp = Aggregator::AVG;
    else if (fFunctionName == "MIN")
        fOp = Aggregator::MIN;
    else if (fFunctionName == "MAX")
        fOp = Aggregator::MAX;  
    
    AFDEBUG << "hashLen=" << fHashLen << " dataLen=" << fDataLen << endl;
    //pthread_mutex_init(&fFifoMutex, NULL);
}

void AggregateFilterStep::doAggFilter()
{
    TupleBucketDataList *inDL = 0;
    
try
{
    if (0 <  fInJSA.status() )
    {
	fOutJSA.status(fInJSA.status());
    }
    else
    {
    inDL = dynamic_cast<TupleBucketDataList*> (fInJSA.outAt(0)->tupleBucketDL());
    if (!inDL)
        throw logic_error("AggregateFilter::input is not TupleBucketDataList");

    // @note only cover one column aggregate function (int and string type) 
    // @todo more complex logic to decide result type for expression.
    assert(fAggMetas.size() == 1);
    std::vector<boost::thread *> runners;	
    int type = -1;
    switch (fOp)
    {
        case Aggregator::SUM:
        case Aggregator::COUNT:
            type = INT64;
            break;
        case Aggregator::MIN:
        case Aggregator::MAX:
        {
            switch (fAggMetas[0].colType.colDataType)
            {
                case CalpontSystemCatalog::BIT:
                case CalpontSystemCatalog::TINYINT:
                case CalpontSystemCatalog::SMALLINT:
                case CalpontSystemCatalog::MEDINT:
                case CalpontSystemCatalog::INT:
                case CalpontSystemCatalog::BIGINT: 
                case CalpontSystemCatalog::DATE:
    	        case CalpontSystemCatalog::DATETIME:
    	        case CalpontSystemCatalog::DECIMAL:
                    type = INT64;
                    break;
                case CalpontSystemCatalog::CHAR:
                case CalpontSystemCatalog::VARCHAR:
                case CalpontSystemCatalog::BLOB:
                case CalpontSystemCatalog::CLOB:
                    if (fAggMetas[0].colType.colWidth > 8)
                        type = STRING;
                    else
                        type = INT64;
                    break;
                case CalpontSystemCatalog::DOUBLE:
                    type = DOUBLE;
                    break;
                case CalpontSystemCatalog::FLOAT:
                    type = FLOAT;
                    break;
				default:
                    type = INT64;
                    break;
            }
        }
        case Aggregator::AVG:
            switch (fAggMetas[0].colType.colDataType)
            {
                case CalpontSystemCatalog::FLOAT:
                    type = FLOAT;
                    break;
                default:
                    type = DOUBLE;
                    break;
            }
            break;
    }    
    
    vector<FilterArg>::iterator filterIt = fFilters.begin();
    
    switch (type)
    {
        case INT64:
        {
            vector<TFilter<int64_t> > filters;
            for (; filterIt != fFilters.end(); filterIt++)
                filters.push_back(TFilter<int64_t> (filterIt->cop, filterIt->intVal));
            startThreads<int64_t> (filters);  
            break;  
        }
        case STRING:
        {
            vector<TFilter<string> > filters;
            for (; filterIt != fFilters.end(); filterIt++)
                filters.push_back(TFilter<string> (filterIt->cop, filterIt->strVal));
            startThreads<string> (filters);        
            break;
        }
        case FLOAT:
        {
            vector<TFilter<float> > filters;
            for (; filterIt != fFilters.end(); filterIt++)
                filters.push_back(TFilter<float> (filterIt->cop, (float)filterIt->intVal));
            startThreads<float> (filters); 
            break;
        }
        case DOUBLE:
        {
            vector<TFilter<double> > filters;
            for (; filterIt != fFilters.end(); filterIt++)
                filters.push_back(TFilter<double> (filterIt->cop, (double)filterIt->intVal));
            startThreads<double> (filters); 
            break;
        }
        default:
            throw logic_error("not supported result type");
    }  
 } // else fInJSA.status == 0
}  //try
catch(const exception& ex)
{
	catchHandler(ex.what());
	fOutJSA.status(logging::aggregateFilterStepErr);
}
catch(...)
{
	string msg("AggregateFilterStep: doAggFilter caught unknown exception");
	catchHandler(msg);
	fOutJSA.status(logging::aggregateFilterStepErr);
}

    // result endofinput
    DataList_t *outDL = fOutJSA.outAt(0)->dataList();
    StrDataList *outStrDL = fOutJSA.outAt(0)->strDataList(); 
	FifoDataList *fifo = fOutJSA.outAt(0)->fifoDL();
	StringFifoDataList *strFifo = fOutJSA.outAt(0)->stringDL();
    uint64_t totalSize;
    
    // @todo check double, float?
    if (fifo)
    {
        fifo->endOfInput();
        totalSize = fFifoRowCount;
        fifo->totalSize(totalSize);
    }
    else if (strFifo)
    {
        strFifo->endOfInput();
        totalSize = fFifoRowCount;
        strFifo->totalSize(totalSize); 
    }
    else if (outDL)
    {
        outDL->endOfInput();
        totalSize = outDL->totalSize();
    }
    else // assume outStrDL
    {
        outStrDL->endOfInput();
        totalSize = outStrDL->totalSize();
    }
      
    dlTimes.setLastReadTime();
    dlTimes.setLastInsertTime();    
    dlTimes.setEndOfInputTime();    
        
    if (fTableOID >= 3000 && traceOn())
    {
        //...Print job step completion information
        time_t t = time(0);
        char timeString[50];
        ctime_r(&t, timeString);
        timeString[strlen(timeString)-1] = '\0';
        
        ostringstream logStr;
        logStr << "ses:" << fSessionID << " st: " << fStepID <<
		" finished at " << timeString <<
        "; output size-" << totalSize << endl <<
        "\t1st Insert "  << dlTimes.FirstInsertTimeString() << " " <<
        "1st read " << dlTimes.FirstReadTimeString() << " " <<
        "last read " << dlTimes.LastReadTimeString() << " " <<
        "EOI " << dlTimes.EndOfInputTimeString() << "; runtime-" <<
            JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(),dlTimes.FirstReadTime())<< "s" << endl;
        
        logEnd(logStr.str().c_str());
        
        syslogProcessingTimes(16,      // exemgr subsystem
            dlTimes.FirstReadTime(),   // first datalist read
            dlTimes.LastReadTime(),    // last  datalist read
            dlTimes.FirstInsertTime(), // first datalist write
            dlTimes.EndOfInputTime()); // last (endOfInput) datalist write
        syslogEndStep(16,              // exemgr subsystem
            0,                         // blocked datalist input
            0);                        // blocked datalist output
    }
}

template<typename result_t>
void AggregateFilterStep::startThreads(vector<TFilter<result_t> >& fv)
{
	fNumThreads = fRm.getTwNumThreads();
		
    std::vector<boost::shared_ptr<boost::thread> > runners;
    boost::shared_ptr<boost::thread> runner;	
    uint8_t i;
    for (i = 0; i < fNumThreads; i++)
    {
        runner.reset(new boost::thread(ThreadedAggFilter<result_t>(this, i, fv)));
        runners.push_back(runner);
    }
    for (i = 0; i < fNumThreads; i++)
        runners[i]->join();
}

template <typename result_t>
void AggregateFilterStep::doThreadedAggFilter(uint8_t threadID, vector<TFilter<result_t> >& filters)
{
    TupleBucketDataList *inDL = dynamic_cast<TupleBucketDataList*> (fInJSA.outAt(0)->tupleBucketDL());
    TupleHasher tupleHash(fHashLen);
    TupleComparator tupleComp(fHashLen);
    typename AggHashMap<result_t>::SHMP shmp;
    shmp.reset(new typename AggHashMap<result_t>::TupleHashMap(1, tupleHash, tupleComp));
    vector<TupleType> vt;
    
    // avg needs to call non-template function to perform div in aggregator
    typename AggHashMap<double>::SHMP shmp_avg;
    vector<TFilter<double> > fs;
    
    
    if (fOp == Aggregator::AVG)
    {
        vector<FilterArg>::iterator filterIt = fFilters.begin();
        shmp_avg.reset(new typename AggHashMap<double>::TupleHashMap(1, tupleHash, tupleComp));        
        for (; filterIt != fFilters.end(); filterIt++)
            fs.push_back(TFilter<double> (filterIt->cop, (double)filterIt->intVal));
    }

    typename AggHashMap<result_t>::TupleHMIter iter;
    typename AggHashMap<double>::TupleHMIter iter_avg;
    
    // to profile accurate time
    if (dlTimes.FirstReadTime().tv_sec==0)    
    {
    	uint64_t totalSize = inDL->totalSize();
    	AFDEBUG << "TotalSize= " << totalSize << endl;
        dlTimes.setFirstReadTime();    
    }
    
    Aggregator *aggregator = new Aggregator (fOp, fAggMetas, fAggParam, fHashLen, 0);
    for (uint64_t i = threadID; i < inDL->bucketCount(); i += fNumThreads)
    {
        aggregator->bucketID(i);
        if (fOp == Aggregator::AVG)
        {
            aggregator->doAggregate_AVG (shmp_avg, inDL, vt);
            doFilter(shmp_avg, fs, vt);
            
            // clean up hashmap memory
            for (iter_avg = shmp_avg->begin(); iter_avg != shmp_avg->end(); iter++)
                delete [] iter_avg->first;
            shmp_avg->clear();
            vt.clear();
        }
        else
        {
            aggregator->doAggregate<result_t> (shmp, inDL, vt);
            doFilter(shmp, filters, vt);

            // clean up memory
            for (iter = shmp->begin(); iter != shmp->end(); ++iter)
                delete [] iter->first;
            shmp->clear();
            vt.clear();            
        }
    }
    delete aggregator;
}

template <typename result_t>
void AggregateFilterStep::doFilter (typename AggHashMap<result_t>::SHMP& shmp, 
                                    vector<TFilter<result_t> >& filters, vector<TupleType> &vt)
{
    // loop through hashtable and do filter
    typename AggHashMap<result_t>::TupleHMIter hashIt = shmp->begin();
    typename vector<TFilter<result_t> >::iterator it;
    DataList_t *outDL = fOutJSA.outAt(0)->dataList();
    StrDataList *outStrDL = fOutJSA.outAt(0)->strDataList(); 
	FifoDataList *fifo = fOutJSA.outAt(0)->fifoDL();
	StringFifoDataList *strFifo = fOutJSA.outAt(0)->stringDL();
    
    bool ret = false;
    
    // erase rids list of unqualified groups from the map
    for (; hashIt != shmp->end(); ++hashIt)
    {
        for (it = filters.begin(); it != filters.end(); it++)
        {
            ret = compare<result_t>(hashIt->second, it->val, it->cop);
            if (!ret)
            { 
                hashIt->first[fHashLen] = DROP;
                break;
            }
        }
    }
    
    if (fifo)
        outputFifo<result_t, UintRowGroup>(shmp, fifo, vt);  
    else if (strFifo)
        outputFifo<result_t, StringRowGroup>(shmp, strFifo, vt);    
    else if (outDL)
        output<result_t, ElementType>(shmp, outDL, vt);
    else // assume outStrDL
        output<result_t, StringElementType>(shmp, outStrDL, vt);         
}

template<typename result_t, typename element_t>
void AggregateFilterStep::output(typename AggHashMap<result_t>::SHMP& shmp,
                                 DataList<element_t> *outDL, vector<TupleType> &vt)
{    
    // @todo may need to separate the function so don't check type every time
    element_t e;
    vector<element_t> v;
    vector<TupleType>::iterator it;
    typename AggHashMap<result_t>::TupleHMIter hashIt;
    
    if (dlTimes.FirstInsertTime().tv_sec==0)    
        dlTimes.setFirstInsertTime();    
        
    for (it = vt.begin(); it != vt.end(); ++it)
    {
        if (it->second[fHashLen] == KEEP)
        {
            switch (fOutputMeta.colType.colDataType)
            {
            case execplan::CalpontSystemCatalog::BIGINT:
            case execplan::CalpontSystemCatalog::INT: {
				ElementType *et = (ElementType *) &e;
                memset(&et->second, 0, 8);
                memcpy(&et->second, it->second+fOutputMeta.startPos, fOutputMeta.colType.colWidth);        
                break;
			}
            default:
                throw logic_error("AggregateFilterStep::output unsupported output column type");
            }
            e.first = it->first;
            v.push_back(e);
            AFDEBUG << "output " << e.first << "/" << e.second << std::endl;
      		if (v.size() >= INSERT_VEC_SIZE)
       		{
       		    outDL->insert(v);
       		    v.clear();
            }
        }
    }
    
    if (v.size() != 0) {
	    outDL->insert(v);
	    v.clear();
	}
}

template<typename result_t, typename element_t>
void AggregateFilterStep::outputFifo(typename AggHashMap<result_t>::SHMP& shmp,
                                 FIFO<element_t> *outDL, vector<TupleType> &vt)
{    
    // @todo may need to separate the function so don't check type every time
    element_t rw;
    vector<TupleType>::iterator it;
    typename AggHashMap<result_t>::TupleHMIter hashIt;
    
    if (dlTimes.FirstInsertTime().tv_sec==0)    
        dlTimes.setFirstInsertTime();    
        
    for (it = vt.begin(); it != vt.end(); ++it)
    {
        if (it->second[fHashLen] == KEEP)
        {
            switch (fOutputMeta.colType.colDataType)
            {
            case execplan::CalpontSystemCatalog::BIGINT:
            case execplan::CalpontSystemCatalog::INT:
                memset(&rw.et[rw.count].second, 0, 8);
                memcpy(&rw.et[rw.count].second, it->second+fOutputMeta.startPos, fOutputMeta.colType.colWidth);        
                break;
            default:
                throw logic_error("AggregateFilterStep::output unsupported output column type");
            }
            
            rw.et[rw.count].first = it->first;							
			AFDEBUG << "output " << rw.et[rw.count].first << "/" << rw.et[rw.count].second << std::endl;
			rw.count++;    
			if (rw.count == rw.ElementsPerGroup)
			{
			    fFifoMutex.lock(); //pthread_mutex_lock(&fFifoMutex);
				outDL->insert(rw);
				fFifoRowCount += rw.count;
				fFifoMutex.unlock(); //pthread_mutex_unlock(&fFifoMutex);
				rw.count = 0;
			}
        }
    }
    
    if (rw.count > 0)
    {
        fFifoMutex.lock(); //pthread_mutex_lock(&fFifoMutex);
		outDL->insert(rw);
		fFifoRowCount += rw.count;
		fFifoMutex.unlock(); //pthread_mutex_unlock(&fFifoMutex);
	}
}

template<typename result_t>
inline bool AggregateFilterStep::compare(result_t& val1, result_t& val2, int8_t COP)
{
    switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return (val1 < val2 ? true : false);
		case COMPARE_EQ:
			return (val1 == val2 ? true : false);
		case COMPARE_LE:
			return (val1 <= val2 ? true : false);
		case COMPARE_GT:
			return (val1 > val2 ? true : false);
		case COMPARE_NE:
			return (val1 != val2 ? true : false);
		case COMPARE_GE:
			return (val1 >= val2 ? true : false);
		default:
			return false;						
	}
}

void AggregateFilterStep::addFilter(int8_t COP, int64_t value, bool isNorm)
{
    FilterArg arg;
    arg.cop = COP;
    arg.intVal = value;
    arg.isNorm = isNorm;
    fFilters.push_back(arg);
    AFDEBUG << "addFilter: COP=" << (uint)COP << " VAL=" << value << std::endl;
}

void AggregateFilterStep::addFilter(int8_t COP, string value, bool isNorm)
{
    FilterArg arg;
    arg.cop = COP;
    arg.strVal = value;
    arg.isNorm = isNorm;
    fFilters.push_back(arg);
    AFDEBUG << "addFilter: COP=" << (uint)COP << " VAL=" << value << std::endl;
}    

void AggregateFilterStep::roundColType(CalpontSystemCatalog::ColType& ct)
{
	if ( ct.colDataType == CalpontSystemCatalog::VARCHAR )
		ct.colWidth++; 
	//If this is a dictionary column, fudge the numbers...
	if (ct.colWidth > 8 )
	    ct.colWidth = 8;	

	//Round colWidth up
	if (ct.colWidth == 3)
		ct.colWidth = 4;
	else if (ct.colWidth == 5 || ct.colWidth == 6 || ct.colWidth == 7)
		ct.colWidth = 8;
}

};  //namespace
