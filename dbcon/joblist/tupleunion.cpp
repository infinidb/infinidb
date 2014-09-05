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
 * $Id: tupleunion.cpp 9674 2013-07-10 16:38:09Z dhall $
 *
 ****************************************************************************/

#include <string>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "dataconvert.h"
#include "hasher.h"
#include "jlf_common.h"
#include "resourcemanager.h"
#include "tupleunion.h"

using namespace std;
using namespace std::tr1;
using namespace boost;
using namespace execplan;
using namespace rowgroup;
using namespace dataconvert;

#ifndef __linux__
#ifndef M_LN10
#define M_LN10 2.30258509299404568402	/* log_e 10 */
#endif
namespace {
//returns the value of 10 raised to the power x.
inline double pow10(double x)
{
	return exp(x * M_LN10);
}
}
#endif

namespace joblist
{
uint64_t TupleUnion::Hasher::operator()(const uint8_t *d) const
{
	return h((const char *) d, ts->rowLength);
}

bool TupleUnion::Eq::operator()(const uint8_t *d1, const uint8_t *d2) const
{
	return (memcmp(d1, d2, ts->rowLength) == 0);
}

TupleUnion::TupleUnion(CalpontSystemCatalog::OID tableOID, const JobInfo& jobInfo) :
	JobStep(jobInfo),
	fTableOID(tableOID),
	isDelivery(false),
	output(NULL),
	outputIt(-1),
	memUsage(0),
	rm(jobInfo.rm),
	allocator(64*1024*1024 + 1),
	runnersDone(0),
	runRan(false),
	joinRan(false)
{
	uniquer.reset(new Uniquer_t(10, Hasher(this), Eq(this), allocator));
}

TupleUnion::~TupleUnion()
{
	rm.returnMemory(memUsage);
	if (!runRan && output)
		output->endOfInput();
}

CalpontSystemCatalog::OID TupleUnion::tableOid() const
{
	return fTableOID;
}

void TupleUnion::setInputRowGroups(const vector<rowgroup::RowGroup> &in)
{
	inputRGs = in;
}

void TupleUnion::setOutputRowGroup(const rowgroup::RowGroup &out)
{
	outputRG = out;
	rowLength = outputRG.getRowSize();
}

void TupleUnion::setDistinctFlags(const vector<bool> &v)
{
	distinctFlags = v;
}

void TupleUnion::setIsDelivery(bool b)
{
	isDelivery = b;
}

void TupleUnion::readInput(uint which)
{
	RowGroupDL *dl = NULL;
	bool more = true;
	shared_array<uint8_t> inRGData, outRGData, tmpRGData;
	uint it = numeric_limits<uint>::max();
	RowGroup l_inputRG, l_outputRG, l_tmpRG;
	Row inRow, outRow, tmpRow;
	bool distinct, inserted;
	uint64_t memUsageBefore, memUsageAfter, memDiff;
// 	uint rCount = 0;

	try {
		l_outputRG = outputRG;
		outRGData.reset(new uint8_t[l_outputRG.getMaxDataSize()]);
		dl = inputs[which];
		l_inputRG = inputRGs[which];
		l_inputRG.initRow(&inRow);
		l_outputRG.initRow(&outRow);
		l_outputRG.setData(outRGData.get());
		l_outputRG.resetRowGroup(0);
		l_outputRG.getRow(0, &outRow);
		distinct = distinctFlags[which];

		if (distinct) {
			l_tmpRG = outputRG;
			tmpRGData.reset(new uint8_t[l_tmpRG.getMaxDataSize()]);
			l_tmpRG.initRow(&tmpRow);
			l_tmpRG.setData(tmpRGData.get());
			l_tmpRG.resetRowGroup(0);
			l_tmpRG.getRow(0, &tmpRow);
		}

		it = dl->getIterator();
		more = dl->next(it, &inRGData);

		while (more && !cancelled()) {
			/*
				normalize each row
				  if distinct flag is set insert into uniquer
				    if row was inserted, put it in the output RG
				  else put it in the output RG
			*/
			l_inputRG.setData(inRGData.get());
			l_inputRG.getRow(0, &inRow);
			if (distinct) {
				memDiff = 0;
				l_tmpRG.resetRowGroup(0);
				l_tmpRG.getRow(0, &tmpRow);
				l_tmpRG.setRowCount(l_inputRG.getRowCount());
				for (uint i = 0; i < l_inputRG.getRowCount(); i++, inRow.nextRow(),
				  tmpRow.nextRow())
					normalize(inRow, &tmpRow);

				l_tmpRG.getRow(0, &tmpRow);
				{
					mutex::scoped_lock lk(uniquerMutex);
					memUsageBefore = allocator.getMemUsage();
					for (uint i = 0; i < l_tmpRG.getRowCount(); i++, tmpRow.nextRow()) {
						memcpy(outRow.getData(), tmpRow.getData(), tmpRow.getSize());
						inserted = uniquer->insert(outRow.getData()).second;
						if (inserted) {
							memDiff += outRow.getSize();
							addToOutput(&outRow, &l_outputRG, true, &outRGData);
						}
					}
					memUsageAfter = allocator.getMemUsage();
					memDiff += (memUsageAfter - memUsageBefore);
					memUsage += memDiff;
				}
				if (!rm.getMemory(memDiff)) {
					fLogger->logMessage(logging::LOG_TYPE_INFO, logging::ERR_UNION_TOO_BIG);
					if (status() == 0) // preserve existing error code
					{
						errorMessage(logging::IDBErrorInfo::instance()->errorMsg(
							logging::ERR_UNION_TOO_BIG));
						status(logging::ERR_UNION_TOO_BIG);
					}
					abort();
				}
			}
			else {
				for (uint i = 0; i < l_inputRG.getRowCount(); i++, inRow.nextRow()) {
					normalize(inRow, &outRow);
					addToOutput(&outRow, &l_outputRG, false, &outRGData);
				}
			}
			more = dl->next(it, &inRGData);
		}
	}
	catch(...)
	{
		if (status() == 0)
		{
			errorMessage("Union step caught an unknown exception.");
			status(logging::unionStepErr);
			fLogger->logMessage(logging::LOG_TYPE_CRITICAL, "Union step caught an unknown exception.");
		}
		abort();
	}

	/* make sure that the input was drained before exiting.  This can happen if the
	query was aborted */
	if (dl && it != numeric_limits<uint>::max())
		while (more)
			more = dl->next(it, &inRGData);

	{
		mutex::scoped_lock lock(sMutex);
		if (l_outputRG.getRowCount() > 0) {
			output->insert(outRGData);
			if (distinctFlags[which])
				rowMemory.push_back(outRGData);
		}
		runnersDone++;
		if (runnersDone == fInputJobStepAssociation.outSize())
			output->endOfInput();
	}
}

uint TupleUnion::nextBand(messageqcpp::ByteStream &bs)
{
	shared_array<uint8_t> mem;
	bool more;
	uint ret = 0;

	bs.restart();
	more = output->next(outputIt, &mem);
	if (more)
		outputRG.setData(mem.get());
	else {
		mem.reset(new uint8_t[outputRG.getEmptySize()]);
		outputRG.setData(mem.get());
		outputRG.resetRowGroup(0);
		outputRG.setStatus(status());
	}

	bs.load(mem.get(), outputRG.getDataSize());
	ret = outputRG.getRowCount();
	return ret;
}

void TupleUnion::addToOutput(Row *r, RowGroup *rg, bool keepit,
	shared_array<uint8_t> *data)
{
	r->nextRow();
	rg->incRowCount();
	if (rg->getRowCount() == 8192) {
		{
			mutex::scoped_lock lock(sMutex);
			output->insert(*data);
			if (keepit)
				rowMemory.push_back(*data);
		}
		data->reset(new uint8_t[rg->getMaxDataSize()]);
		rg->setData(data->get());
		rg->resetRowGroup(0);
		rg->getRow(0, r);
	}
}

void TupleUnion::normalize(const Row &in, Row *out)
{
	uint i;

	out->setRid(0);
	for (i = 0; i < out->getColumnCount(); i++) {
		if (in.isNullValue(i)) {
			writeNull(out, i);
			continue;
		}
		switch (in.getColTypes()[i]) {
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
				switch (out->getColTypes()[i]) {
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
						if (out->getScale(i) || in.getScale(i))
							goto dec1;
						out->setIntField(in.getIntField(i), i);
						break;
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                        if (in.getScale(i))
                            goto dec1;
                        out->setUintField(in.getUintField(i), i);
                        break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR: {
						ostringstream os;
						if (in.getScale(i)) {
							double d = in.getIntField(i);
							d /= pow10(in.getScale(i));
							os.precision(15);
							os << d;
						}
						else
							os << in.getIntField(i);
						out->setStringField(os.str(), i);
						break;
					}
					case CalpontSystemCatalog::DATE:
					case CalpontSystemCatalog::DATETIME:
						throw logic_error("TupleUnion::normalize(): tried to normalize an int to a date or datetime");
                    case CalpontSystemCatalog::FLOAT:
					case CalpontSystemCatalog::UFLOAT: {
						int scale = in.getScale(i);
						if (scale != 0) {
							float f = in.getIntField(i);
							f /= (uint64_t) pow(10.0, scale);
							out->setFloatField(f, i);
						}
						else
							out->setFloatField(in.getIntField(i), i);
						break;
					}
					case CalpontSystemCatalog::DOUBLE:
					case CalpontSystemCatalog::UDOUBLE: {
						int scale = in.getScale(i);
						if (scale != 0) {
							double d = in.getIntField(i);
							d /= (uint64_t) pow(10.0, scale);
							out->setDoubleField(d, i);
						}
						else
							out->setDoubleField(in.getIntField(i), i);
						break;
					}
					case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::UDECIMAL: {
dec1:					uint64_t val = in.getIntField(i);
						int diff = out->getScale(i) - in.getScale(i);
						if (diff < 0)
							val /= (uint64_t) pow((double) 10, (double) -diff);
						else
							val *= (uint64_t) pow((double) 10, (double) diff);
						out->setIntField(val, i);
						break;
					}
					default:
						ostringstream os;
						os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
							<< out->getColTypes()[i];
						throw logic_error(os.str());
				}
				break;
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
                switch (out->getColTypes()[i]) {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                        if (out->getScale(i))
                            goto dec2;
                        out->setIntField(in.getUintField(i), i);
                        break;
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                        out->setUintField(in.getUintField(i), i);
                        break;
                    case CalpontSystemCatalog::CHAR:
                    case CalpontSystemCatalog::VARCHAR: {
                        ostringstream os;
                        if (in.getScale(i)) {
                            double d = in.getUintField(i);
                            d /= pow10(in.getScale(i));
                            os.precision(15);
                            os << d;
                        }
                        else
                            os << in.getUintField(i);
                        out->setStringField(os.str(), i);
                        break;
                    }
                    case CalpontSystemCatalog::DATE:
                    case CalpontSystemCatalog::DATETIME:
                        throw logic_error("TupleUnion::normalize(): tried to normalize an int to a date or datetime");
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::UFLOAT: {
                        int scale = in.getScale(i);
                        if (scale != 0) {
                            float f = in.getUintField(i);
                            f /= (uint64_t) pow(10.0, scale);
                            out->setFloatField(f, i);
                        }
                        else
                            out->setFloatField(in.getUintField(i), i);
                        break;
                    }
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UDOUBLE: {
                        int scale = in.getScale(i);
                        if (scale != 0) {
                            double d = in.getUintField(i);
                            d /= (uint64_t) pow(10.0, scale);
                            out->setDoubleField(d, i);
                        }
                        else
                            out->setDoubleField(in.getUintField(i), i);
                        break;
                    }
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::UDECIMAL: {
dec2:					uint64_t val = in.getIntField(i);
                        int diff = out->getScale(i) - in.getScale(i);
                        if (diff < 0)
                            val /= (uint64_t) pow((double) 10, (double) -diff);
                        else
                            val *= (uint64_t) pow((double) 10, (double) diff);
                        out->setIntField(val, i);
                        break;
                    }
                    default:
                        ostringstream os;
                        os << "TupleUnion::normalize(): tried an illegal conversion: integer to "
                            << out->getColTypes()[i];
                        throw logic_error(os.str());
                }
                break;
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
				switch (out->getColTypes()[i]) {
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
						out->setStringField(in.getStringField(i), i);
						break;
					default: {
						ostringstream os;
						os << "TupleUnion::normalize(): tried an illegal conversion: string to "
							<< out->getColTypes()[i];
						throw logic_error(os.str());
					}
				}
				break;
			case CalpontSystemCatalog::DATE:
				switch (out->getColTypes()[i]) {
					case CalpontSystemCatalog::DATE:
						out->setIntField(in.getIntField(i), i);
						break;
					case CalpontSystemCatalog::DATETIME: {
						uint64_t date = in.getUintField(i);
						date &= ~0x3f;  // zero the 'spare' field
						date <<= 32;
						out->setUintField(date, i);
						break;
					}
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR: {
						string d = DataConvert::dateToString(in.getUintField(i));
						out->setStringField(d, i);
						break;
					}
					default: {
						ostringstream os;
						os << "TupleUnion::normalize(): tried an illegal conversion: date to "
							<< out->getColTypes()[i];
						throw logic_error(os.str());
					}
				}
				break;
			case CalpontSystemCatalog::DATETIME:
				switch(out->getColTypes()[i]) {
					case CalpontSystemCatalog::DATETIME:
						out->setIntField(in.getIntField(i), i);
						break;
					case CalpontSystemCatalog::DATE: {
						uint64_t val = in.getUintField(i);
						val >>= 32;
						out->setUintField(val, i);
						break;
					}
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR: {
						string d = DataConvert::datetimeToString(in.getUintField(i));
						out->setStringField(d, i);
						break;
					}
					default: {
						ostringstream os;
						os << "TupleUnion::normalize(): tried an illegal conversion: datetime to "
							<< out->getColTypes()[i];
						throw logic_error(os.str());
					}
				}
				break;
			case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
            case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE: {
				double val = (in.getColTypes()[i] == CalpontSystemCatalog::FLOAT ?
					in.getFloatField(i) : in.getDoubleField(i));

				switch (out->getColTypes()[i]) {
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
						if (out->getScale(i))
							goto dec3;
						out->setIntField((int64_t) val, i);
						break;
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                        out->setUintField((uint64_t) val, i);
                        break;
					case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::UFLOAT:
						out->setFloatField(val, i);
						break;
					case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UDOUBLE:
						out->setDoubleField(val, i);
						break;
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR: {
						ostringstream os;
						os.precision(15);  // to match mysql's output
						os << val;
						out->setStringField(os.str(), i);
						break;
					}
					case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::UDECIMAL: {
dec3:					/* have to pick a scale to use for the double. using 5... */
						uint scale = 5;
						uint64_t ival = (uint64_t) (double) (val * pow((double) 10, (double) scale));
						int diff = out->getScale(i) - scale;
						if (diff < 0)
							ival /= (uint64_t) pow((double) 10, (double) -diff);
						else
							ival *= (uint64_t) pow((double) 10, (double) diff);
						out->setIntField((int64_t) val, i);
						break;
					}
					default:
						ostringstream os;
						os << "TupleUnion::normalize(): tried an illegal conversion: floating point to "
							<< out->getColTypes()[i];
						throw logic_error(os.str());
				}
				break;
			}
            case CalpontSystemCatalog::DECIMAL:
			case CalpontSystemCatalog::UDECIMAL: {
				int64_t val = in.getIntField(i);
				uint    scale = in.getScale(i);

				switch (out->getColTypes()[i]) {
					case CalpontSystemCatalog::TINYINT:
					case CalpontSystemCatalog::SMALLINT:
					case CalpontSystemCatalog::MEDINT:
					case CalpontSystemCatalog::INT:
					case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::DECIMAL:
					case CalpontSystemCatalog::UDECIMAL: {
						if (out->getScale(i) == scale)
							out->setIntField(val, i);
						else if (out->getScale(i) > scale)
							out->setIntField(IDB_pow[out->getScale(i)-scale]*val, i);
						else // should not happen, the output's scale is the largest
							throw logic_error("TupleUnion::normalize(): incorrect scale setting");

						break;
					}
					case CalpontSystemCatalog::FLOAT: 
					case CalpontSystemCatalog::UFLOAT: {
						float fval = ((float) val) / IDB_pow[scale];
						out->setFloatField(fval, i);
						break;
					}
					case CalpontSystemCatalog::DOUBLE: {
						double dval = ((double) val) / IDB_pow[scale];
						out->setDoubleField(dval, i);
						break;
					}
					case CalpontSystemCatalog::CHAR:
					case CalpontSystemCatalog::VARCHAR:
					default: {
						char buf[50];
						dataconvert::DataConvert::decimalToString(val, scale, buf, 50, out->getColTypes()[i]);
					/*	ostringstream oss;
						if (scale == 0)
							oss << val;
						else
							oss << (val / IDB_pow[scale]) << "."
								<< setw(scale) << setfill('0') << (val % IDB_pow[scale]); */
						out->setStringField(string(buf), i);
						break;
					}
				}
				break;
			}
			case CalpontSystemCatalog::VARBINARY: {
				// out->setVarBinaryField(in.getVarBinaryStringField(i), i);  // not efficient
				out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), i);
				break;
			}
			default: {
				ostringstream os;
				os << "TupleUnion::normalize(): unknown input type (" << in.getColTypes()[i]
					<< ")";
				cout << os << endl;
				throw logic_error(os.str());
			}
		}
	}
}

void TupleUnion::run()
{
	uint i;

	mutex::scoped_lock lk(jlLock);
	if (runRan)
		return;
	runRan = true;
	lk.unlock();
	
	for (i = 0; i < fInputJobStepAssociation.outSize(); i++)
		inputs.push_back(fInputJobStepAssociation.outAt(i)->rowGroupDL());

	output = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
	if (isDelivery) {
		outputIt = output->getIterator();
	}

	for (i = 0; i < inputs.size(); i++) {
		boost::shared_ptr<boost::thread> th(new boost::thread(Runner(this, i)));
		runners.push_back(th);
	}
}

void TupleUnion::join()
{
	uint i;
	mutex::scoped_lock lk(jlLock);
	Uniquer_t::iterator it;

	if (joinRan)
		return;
	joinRan = true;
	lk.unlock();

	for (i = 0; i < runners.size(); i++)
		runners[i]->join();
	runners.clear();
	uniquer->clear();
	rowMemory.clear();
	rm.returnMemory(memUsage);
	memUsage = 0;
}

const string TupleUnion::toString() const
{
	ostringstream oss;
 	oss << "TupleUnion       ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId;
	oss << " st:" << fStepId;
	oss << " in:";
    for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
		oss << ((i==0) ? " " : ", ") << fInputJobStepAssociation.outAt(i);
	oss << " out:";
    for (unsigned i = 0; i < fOutputJobStepAssociation.outSize(); i++)
		oss << ((i==0) ? " " : ", ") << fOutputJobStepAssociation.outAt(i);
	oss << endl;

	return oss.str();

}

void TupleUnion::writeNull(Row *out, uint col)
{
	switch (out->getColTypes()[col]) {
		case CalpontSystemCatalog::TINYINT:
			out->setUintField<1>(joblist::TINYINTNULL, col); break;
		case CalpontSystemCatalog::SMALLINT:
			out->setUintField<1>(joblist::SMALLINTNULL, col); break;
        case CalpontSystemCatalog::UTINYINT:
            out->setUintField<1>(joblist::UTINYINTNULL, col); break;
        case CalpontSystemCatalog::USMALLINT:
            out->setUintField<1>(joblist::USMALLINTNULL, col); break;
		case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
		{
			uint len = out->getColumnWidth(col);
			switch (len)
			{
				case 1: 
					out->setUintField<1>(joblist::TINYINTNULL, col); break;
				case 2: 
					out->setUintField<1>(joblist::SMALLINTNULL, col); break;
				case 4:
					out->setUintField<4>(joblist::INTNULL, col); break;
				case 8:
					out->setUintField<8>(joblist::BIGINTNULL, col); break;
				default: {} 
			}
			break;		
		}
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			out->setUintField<4>(joblist::INTNULL, col); break;
        case CalpontSystemCatalog::UINT:
            out->setUintField<4>(joblist::UINTNULL, col); break;
        case CalpontSystemCatalog::FLOAT:
		case CalpontSystemCatalog::UFLOAT:
			out->setUintField<4>(joblist::FLOATNULL, col); break;
		case CalpontSystemCatalog::DATE:
			out->setUintField<4>(joblist::DATENULL, col); break;
		case CalpontSystemCatalog::BIGINT:
			out->setUintField<8>(joblist::BIGINTNULL, col); break;
        case CalpontSystemCatalog::UBIGINT:
            out->setUintField<8>(joblist::UBIGINTNULL, col); break;
		case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
			out->setUintField<8>(joblist::DOUBLENULL, col); break;
		case CalpontSystemCatalog::DATETIME:
			out->setUintField<8>(joblist::DATETIMENULL, col); break;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR: {
			uint len = out->getColumnWidth(col);
			switch (len) {
				case 1: out->setUintField<1>(joblist::CHAR1NULL, col); break;
				case 2: out->setUintField<2>(joblist::CHAR2NULL, col); break;
				case 3:
				case 4: out->setUintField<4>(joblist::CHAR4NULL, col); break;
				case 5:
				case 6:
				case 7:
				case 8: out->setUintField<8>(joblist::CHAR8NULL, col); break;
				default:
					out->setStringField(joblist::CPNULLSTRMARK, col); break;
			}
			break;
		}
		case CalpontSystemCatalog::VARBINARY:
			// could use below if zero length and NULL are treated the same
			// out->setVarBinaryField("", col); break;
			out->setVarBinaryField(joblist::CPNULLSTRMARK, col); break;
		default: { }
	}
}

}
