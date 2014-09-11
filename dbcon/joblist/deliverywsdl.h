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
// $Id: deliverywsdl.h 7396 2011-02-03 17:54:36Z rdempsey $
// C++ Interface: deliverywsdl
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "elementtype.h"
#include "calpontsystemcatalog.h"
#include <boost/shared_ptr.hpp>
#include <vector>

#ifndef _DELIVERYWSDL_H_
#define _DELIVERYWSDL_H_

namespace joblist
{

/* What should the element type be?  Might be a benefit to using rowgroups */

class DeliveryWSDL : public DataList<ElementType>
{
	public:
		enum ElementMode {
			RID_ONLY,
			RID_VALUE
		};

		DeliveryWSDL(uint numConsumers, uint FIFOMaxElements, uint WSDLMaxElements,
			uint32_t elementSaveSize1st, uint32_t elementSaveSize2nd,
			ResourceManager& rm, execplan::CalpontSystemCatalog::OID tOID,
			execplan::CalpontSystemCatalog::OID *projectingTable);
		virtual ~DeliveryWSDL();

		inline void insert(const ElementType &);
		inline void insert(const std::vector<ElementType> &v);
		uint64_t getIterator();
		inline bool next(uint64_t it, ElementType *e);
		void endOfInput();
		void setMultipleProducers(bool);
		uint32_t getNumConsumers();

		/* Element mode is not currently implemented by DeliveryWSDL. The   */
		/* internal WSDL is always created using ElementType (rid/value).   */
		/* But the mode should still be set in a DeliveryWSDL datalist, in  */
		/* case the datalist is later converted to a ZDL. In that case the  */
		/* element mode may be copied over and used by the ZDL.             */
		void setElementMode(uint mode);
		uint getElementMode() const;

		/* disk I/O accessors */
		bool useDisk();
		void setDiskElemSize(uint32_t size1st,uint32_t size2nd);
		uint32_t getDiskElemSize1st() const;
		uint32_t getDiskElemSize2nd() const;
		uint64_t numberOfTempFiles() const;
		void traceOn(bool b);
        std::list<DiskIoInfo>& diskIoList();
		bool totalDiskIoTime(uint64_t& w, uint64_t& r);
		void totalFileCounts(uint64_t& numFiles, uint64_t& numBytes) const;
		uint64_t totalSize();


	private:
		DeliveryWSDL();
		DeliveryWSDL(const DeliveryWSDL &);
		DeliveryWSDL & operator=(const DeliveryWSDL &);

		boost::shared_ptr<WSDL<ElementType> > wsdl;
		boost::shared_ptr<FIFO<ElementType> > fifo;

		bool doneInsertingIntoWSDL;
		bool deliveringWSDL;
		execplan::CalpontSystemCatalog::OID *projectingTableOID;
		execplan::CalpontSystemCatalog::OID tableOID;

		uint fElementMode;
};

inline void DeliveryWSDL::insert(const ElementType &e)
{
	if (*projectingTableOID == tableOID) {
		if (!doneInsertingIntoWSDL) {
			wsdl->endOfInput();
			doneInsertingIntoWSDL = true;
		}
		fifo->insert(e);
	}
	else
		wsdl->insert(e);
}

inline void DeliveryWSDL::insert(const std::vector<ElementType> &v)
{
	if (*projectingTableOID == tableOID) {
		if (!doneInsertingIntoWSDL) {
			wsdl->endOfInput();
			doneInsertingIntoWSDL = true;
		}
		fifo->insert(v);
	}
	else
		wsdl->insert(v);
}

inline bool DeliveryWSDL::next(uint64_t it, ElementType *e)
{
	bool ret;

	if (deliveringWSDL) {
		ret = wsdl->next(it, e);
		if (!ret)
			deliveringWSDL = false;
		else
			return true;
	}
	return fifo->next(it, e);
}


};

#endif
