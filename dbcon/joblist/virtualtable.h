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

//  $Id: virtualtable.h 6370 2010-03-18 02:58:09Z xlou $


/** @file */

#ifndef VIRTUAL_TABLE_H
#define VIRTUAL_TABLE_H

#include "calpontsystemcatalog.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

namespace joblist
{

class VirtualTable
{
public:
	VirtualTable();
	virtual ~VirtualTable() {}

	virtual void initialize();
	void addColumn(const execplan::SRCP& column);

	void tableOid(const execplan::CalpontSystemCatalog::OID& oid) { fTableOid = oid; }
	const execplan::CalpontSystemCatalog::OID& tableOid() const   { return fTableOid; }

	void name(const std::string& s) { fName = s; }
	const std::string& name() const { return fName; }

	void alias(const std::string& s) { fAlias = s; }
	const std::string& alias() const { return fAlias; }

	void view(const std::string& v) { fView = v; }
	const std::string& view() const { return fView; }

	const std::vector<execplan::SSC>& columns() const { return fColumns; }
	const execplan::CalpontSystemCatalog::OID& columnOid(uint32_t i) const;

	const std::vector<execplan::CalpontSystemCatalog::ColType>& columnTypes() const
		{ return fColumnTypes; }
	void columnType(execplan::CalpontSystemCatalog::ColType& type, uint32_t i);
	const execplan::CalpontSystemCatalog::ColType& columnType(uint32_t i) const;

	const std::map<UniqId, uint32_t>& columnMap() const { return fColumnMap; }

	void varbinaryOK(bool b) { fVarBinOK = b; }
	bool varbinaryOK() const { return fVarBinOK; }

protected:
	execplan::CalpontSystemCatalog::OID fTableOid;
	std::string                         fName;
	std::string                         fAlias;
	std::string                         fView;

	std::vector<execplan::SSC>          fColumns;
	std::vector<execplan::CalpontSystemCatalog::ColType> fColumnTypes;
	std::map<UniqId, uint32_t>          fColumnMap;

	bool                                fVarBinOK;
};

}

#endif  // VIRTUAL_TABLE_H

