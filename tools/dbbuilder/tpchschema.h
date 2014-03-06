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
* $Id: tpchschema.h 2101 2013-01-21 14:12:52Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef TPCHSCHEMA_H
#define TPCHSCHEMA_H

class TpchSchema
{

public:
    void build();
    void buildTpchTables(std::string schema);
    void buildTable();
    void buildMultiColumnIndex();
	void buildIndex(); 
protected:


private:
    void buildTable(std::string createText);
    void createindex(std::string createText);
};

#endif //TPCHSCHEMA_H
