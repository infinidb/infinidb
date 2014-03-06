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
 *   $Id: tdriver.cpp 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *
 ***********************************************************************/
#include <ctime>
#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <cassert>

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "PrimitiveMsg.h"

using namespace std;
using namespace messageqcpp;

void testColByScan()
{
    cout << "Sending COL_BY_SCAN primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = COL_BY_SCAN;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(ColByScanRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    ColByScanRequestHeader colScan;
    colScan.Hdr.SessionID = 1;
    colScan.Hdr.StatementID = 1;
    colScan.Hdr.TransactionID = 0;
    colScan.Hdr.VerID = 0;
    colScan.LBID = 0;
    colScan.PBID = 0;
    colScan.DataSize = 8;
    colScan.OutputType = 1;
    colScan.BOP = 0;
    colScan.NOPS = 0;
    colScan.NVALS = 1;

    Int64 TempData;
    TempData = 1;

    unsigned char message[10000];

    memmove(message, &packetHeader, sizeof(ISMPacketHeader) );
    memmove(message + sizeof(ISMPacketHeader), &colScan, sizeof(ColByScanRequestHeader) );
    memmove(message + sizeof(ISMPacketHeader) + sizeof(ColByScanRequestHeader),  &TempData, sizeof(Int64));

    obs.append((messageqcpp::ByteStream::byte*)message, sizeof(ISMPacketHeader) + sizeof(ColByScanRequestHeader) + sizeof(Int64));

    cout << "Sending to Primitive Server" << endl;
    proc.write( obs );

    // TODO process results
    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();
    int messageLen = ibs.length();
    if (messageLen)
    {
        cout << "Received results" << endl << endl;

        ISMPacketHeader pktHeader;
        ColResultHeader colResult;

        ByteStream::byte* bytePtr = new messageqcpp::ByteStream::byte[messageLen];
        ibs >> bytePtr;
        memmove(((char*)&pktHeader), bytePtr, sizeof(ISMPacketHeader));
        memmove(((char*)&colResult), bytePtr + sizeof(ColResultHeader), sizeof(ColResultHeader) );

        int remaining = messageLen - sizeof(ISMPacketHeader) - sizeof(ColResultHeader);

        cout << "pktHeader.Reserve: " << pktHeader.Reserve << endl;
        cout << "pktHeader.Flow: " << pktHeader.Flow << endl;
        cout << "pktHeader.Command: " << pktHeader.Command << endl;
        cout << "pktHeader.Size: " << pktHeader.Size << endl;
        cout << "pktHeader.Type: " << pktHeader.Type << endl;
        cout << "pktHeader.Source: " << pktHeader.Source << endl;
        cout << "pktHeader.Dest: " << pktHeader.Dest << endl;
        cout << "pktHeader.FinalDest: " << pktHeader.FinalDest << endl;
        cout << "colResult.Hdr.SessionID: " << colResult.Hdr.SessionID << endl;
        cout << "colResult.Hdr.StatementID: " << colResult.Hdr.StatementID << endl;
        cout << "colResult.Hdr.TransactionID: " << colResult.Hdr.TransactionID << endl;
        cout << "colResult.Hdr.VerID: " << colResult.Hdr.VerID << endl;
        cout << "colResult.LBID: " << colResult.LBID << endl;
        cout << "colResult.NVALS: " << colResult.NVALS << endl;
        cout << "colResult.Pad1: " << colResult.Pad1 << endl;
        cout << "colResult.Pad2: " << colResult.Pad2 << endl;

        cout << "Data" << endl;
        cout << "----" << endl << endl;
        cout << "Data Size: " << remaining << endl;

        if (remaining)
        {
            char* data = new char[remaining];
            memmove(data, bytePtr + sizeof(ISMPacketHeader) + sizeof(ColResultHeader), remaining);

            char *ptr = data;

            for(int i = 0; i < remaining; i++)
            {
                for (int j = 0; j < 10 && j < remaining; j++)
                    printf("%02x ", *ptr++);
                printf("\n");
            }

            delete []data;
        }

        delete []bytePtr;
    }

}


void testColByRid()
{
    cout << "Sending COL_BY_RID primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = COL_BY_RID;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(ColByRIDRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    ColByRIDRequestHeader colByRid;
    colByRid.Hdr.SessionID = 1;
    colByRid.Hdr.StatementID = 1;
    colByRid.Hdr.TransactionID = 0;
    colByRid.Hdr.VerID = 0;
    colByRid.LBID = 0;
    colByRid.PBID = 0;
    colByRid.DataSize = 8;
    colByRid.OutputType = 3;
    colByRid.BOP = 0;
    colByRid.NOPS = 0;
    colByRid.NVALS = 1;

    Int64 TempData;
    TempData = 1;

    unsigned char message[10000];

    memmove(message, &packetHeader, sizeof(ISMPacketHeader) );
    memmove(message + sizeof(ISMPacketHeader), &colByRid, sizeof(ColByRIDRequestHeader) );
    memmove(message + sizeof(ISMPacketHeader) + sizeof(ColByRIDRequestHeader),  &TempData, sizeof(Int64));

    obs.append((messageqcpp::ByteStream::byte*)message, sizeof(ISMPacketHeader) + sizeof(ColByRIDRequestHeader) + sizeof(Int64));

    cout << "Sending to Primitive Server" << endl;

    proc.write(obs);

    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();
    int messageLen = ibs.length();
    if (messageLen)
    {

        cout << "Received results" << endl << endl;

        ISMPacketHeader pktHeader;
        ColResultHeader colResult;

        ByteStream::byte* bytePtr = new messageqcpp::ByteStream::byte[messageLen];
        ibs >> bytePtr;
        memmove(((char*)&pktHeader), bytePtr, sizeof(ISMPacketHeader));
        memmove(((char*)&colResult), bytePtr + sizeof(ColResultHeader), sizeof(ColResultHeader) );

        int remaining = messageLen - sizeof(ISMPacketHeader) - sizeof(ColResultHeader);

        cout << "pktHeader.Reserve: " << pktHeader.Reserve << endl;
        cout << "pktHeader.Flow: " << pktHeader.Flow << endl;
        cout << "pktHeader.Command: " << pktHeader.Command << endl;
        cout << "pktHeader.Size: " << pktHeader.Size << endl;
        cout << "pktHeader.Type: " << pktHeader.Type << endl;
        cout << "pktHeader.Source: " << pktHeader.Source << endl;
        cout << "pktHeader.Dest: " << pktHeader.Dest << endl;
        cout << "pktHeader.FinalDest: " << pktHeader.FinalDest << endl;
        cout << "colResult.Hdr.SessionID: " << colResult.Hdr.SessionID << endl;
        cout << "colResult.Hdr.StatementID: " << colResult.Hdr.StatementID << endl;
        cout << "colResult.Hdr.TransactionID: " << colResult.Hdr.TransactionID << endl;
        cout << "colResult.Hdr.VerID: " << colResult.Hdr.VerID << endl;
        cout << "colResult.LBID: " << colResult.LBID << endl;
        cout << "colResult.NVALS: " << colResult.NVALS << endl;
        cout << "colResult.Pad1: " << colResult.Pad1 << endl;
        cout << "colResult.Pad2: " << colResult.Pad2 << endl;

        cout << "Data" << endl;
        cout << "----" << endl << endl;
        cout << "Data Size: " << remaining << endl;

        if (remaining)
        {
            char* data = new char[remaining];
            memmove(data, bytePtr + sizeof(ISMPacketHeader) + sizeof(ColResultHeader), remaining);

            char *ptr = data;

            for(int i = 0; i < remaining; i++)
            {
                for (int j = 0; j < 10 && j < remaining; j++)
                    printf("%02x ", *ptr++);
                printf("\n");
            }

            delete []data;
        }

        delete []bytePtr;

    }

}


void testColAggByScan()
{
    cout << "Sending COL_AGG_BY_SCAN primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = COL_AGG_BY_SCAN;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(ColAggByScanRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    ColAggByScanRequestHeader  colAggByScan;
    colAggByScan.Hdr.SessionID = 1;
    colAggByScan.Hdr.StatementID = 1;
    colAggByScan.Hdr.TransactionID = 0;
    colAggByScan.Hdr.VerID = 0;
    colAggByScan.LBID = 0;
    colAggByScan.PBID = 0;
    colAggByScan.DataSize = 8;
    colAggByScan.OutputType = 1;
    colAggByScan.BOP = 0;
    colAggByScan.NOPS = 0;
    colAggByScan.NVALS = 1;

    Int64 TempData;
    TempData = 1;

    unsigned char message[10000];

    memmove(message, &packetHeader, sizeof(ISMPacketHeader) );
    memmove(message + sizeof(ISMPacketHeader), &colAggByScan, sizeof(ColAggByScanRequestHeader) );
    memmove(message + sizeof(ISMPacketHeader) + sizeof(ColAggByScanRequestHeader),  &TempData, sizeof(Int64));

    obs.append((messageqcpp::ByteStream::byte*)message, sizeof(ISMPacketHeader) + sizeof(ColAggByScanRequestHeader)
        + sizeof(Int64));

    cout << "Sending to Primitive Server" << endl;

    proc.write(obs);

    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();
    int messageLen = ibs.length();
    if (messageLen)
    {
        ISMPacketHeader pktHeader;
        ColAggResultHeader colAggResult;

        cout << "Received results" << endl << endl;

        ByteStream::byte* bytePtr = new messageqcpp::ByteStream::byte[messageLen];
        ibs >> bytePtr;
        memmove(((char*)&pktHeader), bytePtr, sizeof(ISMPacketHeader));
        memmove(((char*)&colAggResult), bytePtr + sizeof(ColAggResultHeader), sizeof(ColAggResultHeader) );

        int remaining = messageLen - sizeof(ISMPacketHeader) - sizeof(ColAggResultHeader);

        cout << "pktHeader.Reserve: " << pktHeader.Reserve << endl;
        cout << "pktHeader.Flow: " << pktHeader.Flow << endl;
        cout << "pktHeader.Command: " << pktHeader.Command << endl;
        cout << "pktHeader.Size: " << pktHeader.Size << endl;
        cout << "pktHeader.Type: " << pktHeader.Type << endl;
        cout << "pktHeader.Source: " << pktHeader.Source << endl;
        cout << "pktHeader.Dest: " << pktHeader.Dest << endl;
        cout << "pktHeader.FinalDest: " << pktHeader.FinalDest << endl;
        cout << "colAggResult.Hdr.SessionID: " << colAggResult.Hdr.SessionID << endl;
        cout << "colAggResult.Hdr.StatementID: " << colAggResult.Hdr.StatementID << endl;
        cout << "colAggResult.Hdr.TransactionID: " << colAggResult.Hdr.TransactionID << endl;
        cout << "colAggResult.Hdr.VerID: " << colAggResult.Hdr.VerID << endl;
        cout << "colAggResult.LBID: " << colAggResult.LBID << endl;
        cout << "colAggResult.MIN: " << colAggResult.MIN << endl;
        cout << "colAggResult.MAX: " << colAggResult.MAX << endl;
        cout << "colAggResult.SUM: " << colAggResult.SUM << endl;
        cout << "colAggResult.SUMOverflow: " << colAggResult.SUMOverflow << endl;
        cout << "colAggResult.NVALS: " << colAggResult.NVALS << endl;
        cout << "colAggResult.Pad1: " << colAggResult.Pad1 << endl;

        cout << "Data" << endl;
        cout << "----" << endl << endl;
        cout << "Data Size: " << remaining << endl;

        if (remaining)
        {
            char* data = new char[remaining];
            memmove(data, bytePtr + sizeof(ISMPacketHeader) + sizeof(ColResultHeader), remaining);

            char *ptr = data;

            for(int i = 0; i < remaining; i++)
            {
                for (int j = 0; j < 10 && j < remaining; j++)
                    printf("%02x ", *ptr++);
                printf("\n");
            }

            delete []data;
        }

        delete []bytePtr;

    }
}


void testColAggByRid()
{
    cout << "Sending COL_AGG_BY_RID primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ByteStream::octbyte value64;
    ByteStream::quadbyte value32;
    ByteStream::doublebyte value16;
    ByteStream::byte value8;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = COL_AGG_BY_RID;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(ColAggByRIDRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    ColAggByRIDRequestHeader colAggByRID;
    colAggByRID.Hdr.SessionID = 1;
    colAggByRID.Hdr.StatementID = 1;
    colAggByRID.Hdr.TransactionID = 0;
    colAggByRID.Hdr.VerID = 0;
    colAggByRID.LBID = 0;
    colAggByRID.PBID = 0;
    colAggByRID.DataSize = 8;
    colAggByRID.OutputType = 1;
    colAggByRID.BOP = 0;
    colAggByRID.NOPS = 0;
    colAggByRID.NVALS = 0;

    cout << "ISMPacketHeader" << endl;
    cout << "---------------" << endl << endl;
    cout << "Reserve: " << packetHeader.Reserve << endl;
    value32 = packetHeader.Reserve;
    obs << value32;

    cout << "Flow: " << packetHeader.Flow << endl;
    value16 = packetHeader.Flow;
    obs << value16;

    cout << "Command: " << packetHeader.Command << endl;
    value8 = packetHeader.Command;
    obs << value8;

    cout << "Size: " << packetHeader.Size << endl;
    value16 = packetHeader.Size;
    obs << value16;

    cout << "Type: " << packetHeader.Type << endl;
    value8 = packetHeader.Type;
    obs << value8;

    cout << "Source: " << packetHeader.Source << endl;
    value64 = packetHeader.Source;
    obs << value64;

    cout << "Dest: " << packetHeader.Dest << endl;
    value64 = packetHeader.Dest;
    obs << value64;

    cout << "FinalDest: " << packetHeader.FinalDest << endl << endl;
    value64 = packetHeader.FinalDest;
    obs << value64;

    cout << "ColAggByRIDRequestHeader.PrimitiveHeader" << endl;
    cout << "--------------------------------------" << endl << endl;
    cout << "SessionID: " << colAggByRID.Hdr.SessionID << endl;
    value16 = colAggByRID.Hdr.SessionID;
    obs << value16;

    cout << "StatementID: " << colAggByRID.Hdr.StatementID << endl;
    value16 = colAggByRID.Hdr.StatementID;
    obs << value16;

    cout << "TransactionID: " << colAggByRID.Hdr.TransactionID << endl;
    value16 = colAggByRID.Hdr.TransactionID;
    obs << value16;

    cout << "VerID: " << colAggByRID.Hdr.VerID << endl;
    value16 = colAggByRID.Hdr.VerID;
    obs << value16;

    cout << "ColAggByRIDRequestHeader" << endl;
    cout << "-------------------------" << endl << endl;

    cout << "LBID: " << colAggByRID.LBID << endl;
    value64 = colAggByRID.LBID;
    obs << value64;

    cout << "PBID: " << colAggByRID.PBID << endl;
    value64 = colAggByRID.PBID;
    obs << value64;

    cout << "DataSize: " << colAggByRID.DataSize << endl;
    value16 = colAggByRID.DataSize;
    obs << value16;

    cout << "OutputType: " << colAggByRID.OutputType << endl;
    value8 = colAggByRID.OutputType;
    obs << value8;

    cout << "BOP: " << colAggByRID.BOP << endl;
    value8 = colAggByRID.BOP;
    obs << value8;

    cout << "NOPS: " << colAggByRID.NOPS << endl;
    value16 = colAggByRID.NOPS;
    obs << value16;

    cout << "NVALS: " << colAggByRID.NVALS << endl;
    value16 = colAggByRID.NVALS;
    obs << value16;

    //obs << TempData;

    cout << "Sending to Primitive Server" << endl;

    proc.write(obs);

    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();
    if (ibs.length() > 0)
    {
        ISMPacketHeader pktHeader;
        ColAggResultHeader colAggResult;

        cout << "Received results" << endl << endl;

        ibs >> value32;
        pktHeader.Reserve = value32;

        ibs >> value16;
        pktHeader.Flow = value16;

        ibs >> value8;
        pktHeader.Command = value8;

        ibs >> value16;
        pktHeader.Size = value16;

        ibs >> value8;
        pktHeader.Type = value8;

        ibs >> value64;
        pktHeader.Source = value64;

        ibs >> value64;
        pktHeader.Dest = value64;

        ibs >> value64;
        pktHeader.FinalDest = value64;

        ibs >> value16;
        colAggResult.Hdr.SessionID = value16;

        ibs >> value16;
        colAggResult.Hdr.StatementID = value16;

        ibs >> value16;
        colAggResult.Hdr.TransactionID = value16;

        ibs >> value16;
        colAggResult.Hdr.VerID = value16;

        ibs >> value64;
        colAggResult.LBID = value64;

        ibs >> value64;
        colAggResult.MIN = value64;

        ibs >> value64;
        colAggResult.MAX = value64;

        ibs >> value64;
        colAggResult.SUM = value64;

        ibs >> value32;
        colAggResult.SUMOverflow = value32;

        ibs >> value16;
        colAggResult.NVALS = value16;

        ibs >> value16;
        colAggResult.Pad1 = value16;

        cout << "pktHeader.Reserve: " << pktHeader.Reserve << endl;
        cout << "pktHeader.Flow: " << pktHeader.Flow << endl;
        cout << "pktHeader.Command: " << pktHeader.Command << endl;
        cout << "pktHeader.Size: " << pktHeader.Size << endl;
        cout << "pktHeader.Type: " << pktHeader.Type << endl;
        cout << "pktHeader.Source: " << pktHeader.Source << endl;
        cout << "pktHeader.Dest: " << pktHeader.Dest << endl;
        cout << "pktHeader.FinalDest: " << pktHeader.FinalDest << endl;
        cout << "colAggResult.Hdr.SessionID: " << colAggResult.Hdr.SessionID << endl;
        cout << "colAggResult.Hdr.StatementID: " << colAggResult.Hdr.StatementID << endl;
        cout << "colAggResult.Hdr.TransactionID: " << colAggResult.Hdr.TransactionID << endl;
        cout << "colAggResult.Hdr.VerID: " << colAggResult.Hdr.VerID << endl;
        cout << "colAggResult.LBID: " << colAggResult.LBID << endl;
        cout << "colAggResult.MIN: " << colAggResult.MIN << endl;
        cout << "colAggResult.MAX: " << colAggResult.MAX << endl;
        cout << "colAggResult.SUM: " << colAggResult.SUM << endl;
        cout << "colAggResult.SUMOverflow: " << colAggResult.SUMOverflow << endl;
        cout << "colAggResult.NVALS: " << colAggResult.NVALS << endl;
        cout << "colAggResult.Pad1: " << colAggResult.Pad1 << endl;

        cout << "Data" << endl;
        cout << "----" << endl << endl;
        cout << "Data Size: " << ibs.length() << endl;
        for (int i = 0; i < colAggResult.NVALS && ibs.length(); i++)
        {
            if (colAggByRID.OutputType == 1 || colAggByRID.OutputType == 3 )
            {
                ibs >> value16;
                cout << "RID: " << value16 << endl;
            }
            if (colAggByRID.OutputType == 2 || colAggByRID.OutputType == 3)
            {
                cout << "Token: ";
                switch (colAggByRID.DataSize)
                {
                    case 1:
                        ibs >> value8;
                        cout << value8 << endl;
                        break;

                    case 2:
                        ibs >> value16;
                        cout << value16 << endl;
                        break;

                    case 4:
                        ibs >> value32;
                        cout << value32 << endl;
                        break;

                    case 8:
                    default:
                        ibs >> value64;
                        cout << value64 << endl;
                        break;
                }
            }
        }

    }

}


void testDictTokenByIndexCompare()
{
    cout << "Sending DICT_TOKEN_BY_INDEX_COMPARE primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ByteStream::octbyte value64;
    ByteStream::quadbyte value32;
    ByteStream::doublebyte value16;
    ByteStream::byte value8;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = DICT_TOKEN_BY_INDEX_COMPARE;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(DictTokenByIndexRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    DictTokenByIndexRequestHeader  dictTokenByIndex;
    dictTokenByIndex.Hdr.SessionID = 1;
    dictTokenByIndex.Hdr.StatementID = 1;
    dictTokenByIndex.Hdr.TransactionID = 0;
    dictTokenByIndex.Hdr.VerID = 0;
    dictTokenByIndex.LBID  = 0;
    dictTokenByIndex.PBID  = 0;
    dictTokenByIndex.NVALS = 0;
    dictTokenByIndex.Pad1  = 0;
    dictTokenByIndex.Pad2  = 0;

    cout << "ISMPacketHeader" << endl;
    cout << "---------------" << endl << endl;
    cout << "Reserve: " << packetHeader.Reserve << endl;
    value32 = packetHeader.Reserve;
    obs << value32;

    cout << "Flow: " << packetHeader.Flow << endl;
    value16 = packetHeader.Flow;
    obs << value16;

    cout << "Command: " << packetHeader.Command << endl;
    value8 = packetHeader.Command;
    obs << value8;

    cout << "Size: " << packetHeader.Size << endl;
    value16 = packetHeader.Size;
    obs << value16;

    cout << "Type: " << packetHeader.Type << endl;
    value8 = packetHeader.Type;
    obs << value8;

    cout << "Source: " << packetHeader.Source << endl;
    value64 = packetHeader.Source;
    obs << value64;

    cout << "Dest: " << packetHeader.Dest << endl;
    value64 = packetHeader.Dest;
    obs << value64;

    cout << "FinalDest: " << packetHeader.FinalDest << endl << endl;
    value64 = packetHeader.FinalDest;
    obs << value64;

    cout << "DictTokenByIndexRequestHeader.PrimitiveHeader" << endl;
    cout << "--------------------------------------" << endl << endl;
    cout << "SessionID: " << dictTokenByIndex.Hdr.SessionID << endl;
    value16 = dictTokenByIndex.Hdr.SessionID;
    obs << value16;

    cout << "StatementID: " << dictTokenByIndex.Hdr.StatementID << endl;
    value16 = dictTokenByIndex.Hdr.StatementID;
    obs << value16;

    cout << "TransactionID: " << dictTokenByIndex.Hdr.TransactionID << endl;
    value16 = dictTokenByIndex.Hdr.TransactionID;
    obs << value16;

    cout << "VerID: " << dictTokenByIndex.Hdr.VerID << endl;
    value16 = dictTokenByIndex.Hdr.VerID;
    obs << value16;

    cout << "DictTokenByIndexRequestHeader" << endl;
    cout << "-------------------------" << endl << endl;

    cout << "LBID: " << dictTokenByIndex.LBID << endl;
    value64 = dictTokenByIndex.LBID;
    obs << value64;

    cout << "PBID: " << dictTokenByIndex.PBID << endl;
    value64 = dictTokenByIndex.PBID;
    obs << value64;

    cout << "NVALS: " << dictTokenByIndex.NVALS << endl;
    value16 = dictTokenByIndex.NVALS;
    obs << value16;

    cout << "Pad1: " << dictTokenByIndex.Pad1 << endl;
    value16 = dictTokenByIndex.Pad1;
    obs << value16;

    cout << "Pad2: " << dictTokenByIndex.Pad2 << endl;
    value32 = dictTokenByIndex.Pad2;
    obs << value32;

    cout << "Sending to Primitive Server" << endl;

    proc.write(obs);

    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();
    cout << "Data" << endl;
    cout << "----" << endl << endl;
    if (ibs.length() > 0)
    {
        cout << "Data Size: " << ibs.length() << endl;
        while (ibs.length())
        {
            ibs >> value16;
            cout << value16 << endl;
        }
    }
}


void testDictTokenByScanCompare()
{
    cout << "Sending DICT_TOKEN_BY_SCAN_COMPARE primitive" << endl;
}


void testDictSignature()
{
    cout << "DICT_SIGNATURE primitive" << endl;

    MessageQueueClient proc("PMS1");
    ByteStream obs,ibs;

    ByteStream::octbyte value64;
    ByteStream::quadbyte value32;
    ByteStream::doublebyte value16;
    ByteStream::byte value8;

    ISMPacketHeader packetHeader;
    packetHeader.Reserve = 0;
    packetHeader.Flow = 0;
    packetHeader.Command = DICT_SIGNATURE;
    packetHeader.Size = (sizeof(ISMPacketHeader) + sizeof(DictSignatureRequestHeader));
    packetHeader.Type = 2;
    packetHeader.Source = 0;
    packetHeader.Dest = 0;
    packetHeader.FinalDest = 0;

    DictSignatureRequestHeader  dictSignature;
    dictSignature.Hdr.SessionID = 1;
    dictSignature.Hdr.StatementID = 1;
    dictSignature.Hdr.TransactionID = 0;
    dictSignature.Hdr.VerID = 0;
    dictSignature.LBID = 0;
    dictSignature.PBID = 0;
    dictSignature.NVALS = 0;
    dictSignature.Pad1 = 0;
    dictSignature.Pad2 = 0;

    cout << "ISMPacketHeader" << endl;
    cout << "---------------" << endl << endl;
    cout << "Reserve: " << packetHeader.Reserve << endl;
    value32 = packetHeader.Reserve;
    obs << value32;

    cout << "Flow: " << packetHeader.Flow << endl;
    value16 = packetHeader.Flow;
    obs << value16;

    cout << "Command: " << packetHeader.Command << endl;
    value8 = packetHeader.Command;
    obs << value8;

    cout << "Size: " << packetHeader.Size << endl;
    value16 = packetHeader.Size;
    obs << value16;

    cout << "Type: " << packetHeader.Type << endl;
    value8 = packetHeader.Type;
    obs << value8;

    cout << "Source: " << packetHeader.Source << endl;
    value64 = packetHeader.Source;
    obs << value64;

    cout << "Dest: " << packetHeader.Dest << endl;
    value64 = packetHeader.Dest;
    obs << value64;

    cout << "FinalDest: " << packetHeader.FinalDest << endl << endl;
    value64 = packetHeader.FinalDest;
    obs << value64;

    cout << "DictSignatureRequestHeader.PrimitiveHeader" << endl;
    cout << "--------------------------------------" << endl << endl;
    cout << "SessionID: " << dictSignature.Hdr.SessionID << endl;
    value16 = dictSignature.Hdr.SessionID;
    obs << value16;

    cout << "StatementID: " << dictSignature.Hdr.StatementID << endl;
    value16 = dictSignature.Hdr.StatementID;
    obs << value16;

    cout << "TransactionID: " << dictSignature.Hdr.TransactionID << endl;
    value16 = dictSignature.Hdr.TransactionID;
    obs << value16;

    cout << "VerID: " << dictSignature.Hdr.VerID << endl;
    value16 = dictSignature.Hdr.VerID;
    obs << value16;

    cout << "DictSignatureRequestHeader" << endl;
    cout << "-------------------------" << endl << endl;

    cout << "LBID: " << dictSignature.LBID << endl;
    value64 = dictSignature.LBID;
    obs << value64;

    cout << "PBID: " << dictSignature.PBID << endl;
    value64 = dictSignature.LBID;
    obs << value64;

    cout << "NVALS: " << dictSignature.NVALS << endl;
    value16 = dictSignature.NVALS;
    obs << value16;

    cout << "Pad1: " << dictSignature.Pad1 << endl;
    value16 = dictSignature.Pad1;
    obs << value16;

    cout << "Pad2: " << dictSignature.Pad2 << endl;
    value32 = dictSignature.Pad2;
    obs << value32;

    cout << "Sending to Primitive Server" << endl;

    proc.write(obs);

    cout << "Sent... awaiting results" << endl;

    ibs = proc.read();

    cout << "Data" << endl;
    cout << "----" << endl << endl;
    if (ibs.length() > 0)
    {
        cout << "Data Size: " << ibs.length() << endl;
        while (ibs.length())
        {
            ibs >> value16;
            cout << value16 << endl;
        }
    }

}


void testDictAggregate()
{
    cout << "DICT_AGGREGATE primitive" << endl;

}


void testIndexByCompare()
{
    cout << "INDEX_BY_COMPARE primitive" << endl;
}


void testIndexByScan()
{
    cout << "INDEX_BY_SCAN primitive" << endl;
}


int main(int argc, char* argv[])
{

    po::options_description desc ("Allowed options");
    desc.add_options ()
        ("help", "produce help message")
        ("all", "process all tests" )
        ("loop", "loop processing all tests");
    po::variables_map vm;
    po::store (po::parse_command_line (argc, argv, desc), vm);
    po::notify (vm);

    if (vm.count ("all"))
    {
        testColByScan();
        //testColByRid();
        //testColAggByScan();
        //testColAggByRid();
        //testDictSignature();
        //testDictTokenByIndexCompare();
    }
    else if (vm.count ("loop"))
    {
        while(1)
        {
            testColByScan();
            testColByRid();
            testColAggByScan();
            testColAggByRid();
            testDictSignature();
            testDictTokenByIndexCompare();
        }

    }

}
