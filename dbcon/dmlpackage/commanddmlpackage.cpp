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

/***********************************************************************
 *   $Id: commanddmlpackage.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
#include <stdexcept>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>
using namespace std;

#define COMMANDDMLPKG_DLLEXPORT
#include "commanddmlpackage.h"
#undef COMMANDDMLPKG_DLLEXPORT
namespace dmlpackage
{

    CommandDMLPackage::CommandDMLPackage()
        {}

    CommandDMLPackage::CommandDMLPackage( std::string dmlStatement, int sessionID)
        :CalpontDMLPackage( "", "", dmlStatement, sessionID)
        {}

    CommandDMLPackage::~CommandDMLPackage()
        {}

    int CommandDMLPackage::write(messageqcpp::ByteStream& bytestream)
    {
        int retval = 1;

        messageqcpp::ByteStream::byte package_type = DML_COMMAND;
        bytestream << package_type;

        messageqcpp::ByteStream::quadbyte session_id = fSessionID;
        bytestream << session_id;

        bytestream << fUuid;

        bytestream << fDMLStatement;
        bytestream << fSQLStatement; // for cleartablelock, this is table lockID
        bytestream << (uint8_t)fLogging;
        bytestream << fSchemaName;
        bytestream << fTableName;
        bytestream << fTableOid;
        bytestream << static_cast<const messageqcpp::ByteStream::byte>(fIsAutocommitOn);
        bytestream << static_cast<const messageqcpp::ByteStream::byte>(fIsBatchInsert);
        return retval;
    }

    int CommandDMLPackage::read(messageqcpp::ByteStream& bytestream)
    {
        int retval = 1;

        messageqcpp::ByteStream::quadbyte session_id;
        bytestream >> session_id;
        fSessionID = session_id;
        bytestream >> fUuid;

        bytestream >> fDMLStatement;
        bytestream >> fSQLStatement; // for cleartablelock, this is table lockID
        uint8_t logging;
        bytestream >> logging;
        fLogging = (logging != 0);
        bytestream >> fSchemaName;
        bytestream >> fTableName;
        bytestream >> fTableOid;
        bytestream >> reinterpret_cast< messageqcpp::ByteStream::byte&>(fIsAutocommitOn);
        bytestream >> reinterpret_cast< messageqcpp::ByteStream::byte&>(fIsBatchInsert);
        return retval;
    }

    int CommandDMLPackage::buildFromSqlStatement(SqlStatement& sqlStatement)
    {
        CommandSqlStatement& cmdStmt = dynamic_cast<CommandSqlStatement&>(sqlStatement);
        fDMLStatement = cmdStmt.fCommandText;

        return 1;
    }

}                                                 // namespace dmlpackage
