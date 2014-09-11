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
// $Id: deliverywsdl.cpp 7396 2011-02-03 17:54:36Z rdempsey $
// C++ Implementation: deliverywsdl
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "deliverywsdl.h"

namespace joblist
{

DeliveryWSDL::DeliveryWSDL(uint numConsumers, uint FIFOMaxElements, uint WSDLMaxElements,
	uint32_t elementSaveSize1st, uint32_t elementSaveSize2nd,
	ResourceManager& rm, execplan::CalpontSystemCatalog::OID tOID,
	execplan::CalpontSystemCatalog::OID *projectingTable)
{
	fifo.reset(new FIFO<ElementType>(numConsumers, FIFOMaxElements));
	wsdl.reset(new WSDL<ElementType>(numConsumers, WSDLMaxElements, elementSaveSize1st,
		elementSaveSize2nd, rm));

	projectingTableOID = projectingTable;
	doneInsertingIntoWSDL = false;
	deliveringWSDL = true;
	tableOID = tOID;
	fElementMode = RID_VALUE;
}

DeliveryWSDL::~DeliveryWSDL() { }

uint64_t DeliveryWSDL::getIterator()
{
	uint64_t wIt, fIt;

	wIt = wsdl->getIterator();
	fIt = fifo->getIterator();
	assert(wIt == fIt);   // there might be a small race between consumers here
	return wIt;
}

void DeliveryWSDL::endOfInput()
{
	if (!doneInsertingIntoWSDL)
		wsdl->endOfInput();
	fifo->endOfInput();
}

void DeliveryWSDL::setMultipleProducers(bool b)
{
	wsdl->setMultipleProducers(b);
	fifo->setMultipleProducers(b);
}

// Note: ElementMode is not technically implemented in this class; at least not
// at this time.  However, we provide a hook for it to store the element mode,
// because if a HashJoin decides to perform a join using a LHJ, this Delivery-
// WSDL may get changed to a ZDL.  In that case the elementMode can be honored.
// So we want to store and track the "potential" elementMode so that HashJoin
// will be able to access this information if needed.
void DeliveryWSDL::setElementMode(uint elementMode)
{
	fElementMode = elementMode;
}
uint DeliveryWSDL::getElementMode() const
{
	return fElementMode;
}

bool DeliveryWSDL::useDisk()
{
	return true;
}

void DeliveryWSDL::setDiskElemSize(uint32_t size1st, uint32_t size2nd)
{
	wsdl->setDiskElemSize(size1st, size2nd);
}

uint32_t DeliveryWSDL::getDiskElemSize1st() const
{
	return wsdl->getDiskElemSize1st();
}

uint32_t DeliveryWSDL::getDiskElemSize2nd() const
{
	return wsdl->getDiskElemSize2nd();
}

uint64_t DeliveryWSDL::numberOfTempFiles() const
{
	return wsdl->numberOfTempFiles();
}

void DeliveryWSDL::traceOn(bool b)
{
	wsdl->traceOn(b);
}

std::list<DiskIoInfo>& DeliveryWSDL::diskIoList()
{
	return wsdl->diskIoList();
}

bool DeliveryWSDL::totalDiskIoTime(uint64_t &w, uint64_t &r)
{
	return wsdl->totalDiskIoTime(w, r);
}

uint32_t DeliveryWSDL::getNumConsumers()
{
	return wsdl->getNumConsumers();
}

void DeliveryWSDL::totalFileCounts(uint64_t& numFiles, uint64_t& numBytes) const
{
	numFiles = wsdl->numberOfTempFiles();
	numBytes = wsdl->saveSize;
}

uint64_t DeliveryWSDL::totalSize()
{
	return (wsdl->totalSize() + fifo->totalSize());
}

};
