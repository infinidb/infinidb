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

/*
* $Id: primitivemsg.h 9655 2013-06-25 23:08:13Z xlou $
*/

/** @file */

#ifndef JOBLIST_PRIMITIVE_H
#define JOBLIST_PRIMITIVE_H

#include <sys/types.h>

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "joblisttypes.h"

#ifdef __cplusplus
#include <vector>
extern "C"
{
#endif

#pragma pack(push,1)

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4200)
#endif

// from blocksize.h
const int32_t DATA_BLOCK_SIZE=BLOCK_SIZE;

const int8_t COMPARE_NIL=0x00;
const int8_t COMPARE_LT=0x01;
const int8_t COMPARE_EQ=0x02;
const int8_t COMPARE_LE=(COMPARE_LT|COMPARE_EQ); //0x03
const int8_t COMPARE_GT=0x04;
const int8_t COMPARE_NE=(COMPARE_LT|COMPARE_GT); //0x05
const int8_t COMPARE_GE=(COMPARE_GT|COMPARE_EQ); //0x06
const int8_t COMPARE_NOT=0x08;
const int8_t COMPARE_NLT=(COMPARE_LT|COMPARE_NOT); //0x09
const int8_t COMPARE_NLE=(COMPARE_LE|COMPARE_NOT); //0x0b
const int8_t COMPARE_NGT=(COMPARE_GT|COMPARE_NOT); //0x0c
const int8_t COMPARE_NGE=(COMPARE_GE|COMPARE_NOT); //0x0e

const int8_t COMPARE_LIKE=0x10;
const int8_t COMPARE_NLIKE=(COMPARE_LIKE|COMPARE_NOT); //0x18

//      BOP (Binary Operation) values
//          used to tell if the operations are all be true or
//          any to be true.

#define BOP_NONE 0
#define BOP_AND 1
#define BOP_OR 2
#define BOP_XOR 3

//		OT (Output Type) values
// 1 = RID, 2 = Token, 3 = Both

#define OT_RID 1
#define OT_TOKEN 2
#define OT_BOTH 3				// both = RID & TOKEN
#define OT_DATAVALUE 4
#define OT_INPUTARG OT_RID		// reuse OT_RID's flag in dictionary primitives.  Specifies that
								// the filter argument that matched should be part of the result set.
								// (only makes sense when BOP = OR).
#define OT_AGGREGATE 8			// specifies that aggregate data should be generated

//      Packet Header Types

enum PACKETTYPE
{
	NULL1   = 0,
	NULL2   = 0X0F,
	DATA    = 1,
	CMD     = 2,
	FLOW    = 3,
	CONFIG  = 9
};

enum TYPEFLOWCOMMAND
{
	ACK     = 1,
	NACK    = 2
};

//      Define the ISM Commands

#define PRIM_LOCALBASE 10
#define PRIM_COLBASE 50
#define PRIM_INDEXBASE 100
#define PRIM_CACHEBASE 190
#define PRIM_DICTBASE 200
#define PRIM_DELIVERBASE 250

//8 bits only!
enum ISMPACKETCOMMAND
{
	//max of 50-10=40 commands
	LOCAL_JOIN_BY_RID           = PRIM_LOCALBASE+0,
	LOCAL_OR_BY_RID             = PRIM_LOCALBASE+1,
    ARITHMETIC_FUNCTION         = PRIM_LOCALBASE+2,
    FUNCTION_CALL               = PRIM_LOCALBASE+3,
    FUNCTION_CALL_VECTOR        = PRIM_LOCALBASE+4,
    LOCAL_COMPARE_BY_VALUE      = PRIM_LOCALBASE+5,
    LOCAL_JOIN_BY_VALUE         = PRIM_LOCALBASE+6,
	BATCH_PRIMITIVE_CREATE      = PRIM_LOCALBASE+7,
	BATCH_PRIMITIVE_RUN         = PRIM_LOCALBASE+8,
	BATCH_PRIMITIVE_DESTROY     = PRIM_LOCALBASE+9,
	BATCH_PRIMITIVE_ADD_JOINER  = PRIM_LOCALBASE+10,
	BATCH_PRIMITIVE_END_JOINER  = PRIM_LOCALBASE+11,
	BATCH_PRIMITIVE_ACK			= PRIM_LOCALBASE+12,
	BATCH_PRIMITIVE_ABORT		= PRIM_LOCALBASE+13,

	//max of 100-50=50 commands
	COL_RESULTS                 = PRIM_COLBASE+0,
	COL_AGG_RESULTS             = PRIM_COLBASE+1,
	COL_BY_SCAN                 = PRIM_COLBASE+2,
	COL_BY_RID                  = PRIM_COLBASE+3,
	COL_AGG_BY_SCAN             = PRIM_COLBASE+4,
	COL_AGG_BY_RID              = PRIM_COLBASE+5,
	COL_JOIN_BY_SCAN            = PRIM_COLBASE+6,
	COL_FILTER_BY_TOKEN         = PRIM_COLBASE+7,
	COL_FILTER_BY_RID_VAL       = PRIM_COLBASE+8,
   	COL_BY_SCAN_RANGE           = PRIM_COLBASE+9,
   	COL_LOOPBACK                = PRIM_COLBASE+10,

	//max of 190-100=90 commands
	INDEX_RESULTS                = PRIM_INDEXBASE+0, // Obsolete ?
	INDEX_SCAN_RESULTS           = PRIM_INDEXBASE+1, // p_IdxScan results
	INDEX_WALK_RESULTS           = PRIM_INDEXBASE+2, // p_IdxWalk results
	INDEX_LIST_RESULTS           = PRIM_INDEXBASE+3, // p_IdxList results
	INDEX_LIST_AGGREGATE_RESULTS = PRIM_INDEXBASE+4, // p_IdxListAggregate results
	INDEX_SCAN_AGGREGATE_RESULTS = PRIM_INDEXBASE+5, // p_IdxScanAggregate results
	INDEX_BY_SCAN                = PRIM_INDEXBASE+6, // p_IdxScan
	INDEX_BY_COMPARE             = PRIM_INDEXBASE+7, // Obsolete ?
	INDEX_WALK                   = PRIM_INDEXBASE+8, // p_IdxWalk
	INDEX_LIST                   = PRIM_INDEXBASE+9, // p_IdxList
	INDEX_LIST_AGGREGATE         = PRIM_INDEXBASE+10, // p_IdxListAggregate
	INDEX_SCAN_AGGREGATE         = PRIM_INDEXBASE+11, // p_IdxScanAggregate

	//max of 200-190=10 commands
	CACHE_OP_RESULTS           = PRIM_CACHEBASE+0, // Response msg
	CACHE_FLUSH                = PRIM_CACHEBASE+1, // Flush the entire block cache
	CACHE_CLEAN_VSS            = PRIM_CACHEBASE+2, // Clean out indicated VSS entries
	CACHE_DROP_FDS             = PRIM_CACHEBASE+3, // Drop the whole file descriptor cache
	FLUSH_ALL_VERSION          = PRIM_CACHEBASE+4, // Drop all versions of specified LBIDs
	CACHE_FLUSH_BY_OID		   = PRIM_CACHEBASE+5, // Drop all versions of all LBIDs for the given OIDs
	CACHE_FLUSH_PARTITION	   = PRIM_CACHEBASE+6, // Drop a partition
	CACHE_PURGE_FDS			   = PRIM_CACHEBASE+7, // Purge the file descriptor cache for the modified files

	//max of 250-200=50 commands
	DICT_RESULTS                = PRIM_DICTBASE+0,
	DICT_TOKEN_BY_INDEX_COMPARE = PRIM_DICTBASE+1,
	DICT_TOKEN_BY_SCAN_COMPARE  = PRIM_DICTBASE+2,
	DICT_SIGNATURE              = PRIM_DICTBASE+3,
	DICT_AGGREGATE              = PRIM_DICTBASE+4,
	DICT_AGGREGATE_RESULTS		= PRIM_DICTBASE+5,
	DICT_SCAN_COMPARE_RESULTS	= PRIM_DICTBASE+6,
	DICT_SIGNATURE_RANGE        = PRIM_DICTBASE+7,
	DICT_CREATE_EQUALITY_FILTER	= PRIM_DICTBASE+8,
	DICT_DESTROY_EQUALITY_FILTER = PRIM_DICTBASE+9,

	//max of 256-250=6 commands
	DELIVER_TOKEN_RESULTS       = PRIM_DELIVERBASE+0,
	DELIVER_RID_RESULTS         = PRIM_DELIVERBASE+1
};

#undef PRIM_LOCALBASE
#undef PRIM_COLBASE
#undef PRIM_INDEXBASE
#undef PRIM_DICTBASE
#undef PRIM_DELIVERBASE

/* Flags for BPP messages */
const uint8_t NEED_STR_VALUES       = 0x01; //1;
const uint8_t GOT_ABS_RIDS          = 0x02; //2;
const uint8_t GOT_VALUES            = 0x04; //4;
const uint8_t LBID_TRACE            = 0x08; //8;
const uint8_t HAS_JOINER            = 0x10; //16;
const uint8_t SEND_RIDS_AT_DELIVERY = 0x20; //32;
const uint8_t HAS_ROWGROUP          = 0x40; //64;
const uint8_t JOIN_ROWGROUP_DATA	= 0x80; //128

//TODO: put this in a namespace to stop global ns pollution
enum PrimFlags
{
	PF_LBID_TRACE = 0x01,	/*!< Enable LBID tracing in PrimProc */
	PF_PM_PROF = 0x02,	/*!< Enable LBID tracing in PrimProc */
};

enum BPSOutputType {
	BPS_ELEMENT_TYPE,
	STRING_ELEMENT_TYPE,
	TABLE_BAND,
	TUPLE,
	ROW_GROUP
};

//      Constant Message Header structures

//      Packet Header for VBEU & CachEU

#if 0
struct VBCPacketHeader
{
	//    unsigned char Type:4;
	//    unsigned char SubType:4;
	//    unsigned int  Size:16;
	//    unsigned int  Dest:16;
	unsigned int  Source:16;
	unsigned char CmdAddr:8;
};
#endif

//      Packet Header for ISM SubBlock EU

struct ISMPacketHeader
{
	ISMPacketHeader(): Interleave(0), Flags(0), Command(0), Size(0), Type(0), MsgCount(0), Status(0) {}
	uint32_t Interleave;
	uint16_t Flags;
	uint8_t Command;
	uint16_t Size;
	unsigned Type:4;
	unsigned MsgCount:4;
	uint16_t Status;

};

//      Primitive request/response structure Header
//@Bug 2744 changed all variables to 32 bit, and took out StatementID
struct PrimitiveHeader
{
	uint32_t SessionID;     // Front end Session Identifier
	uint32_t TransactionID; // Front end Transaction Identifier
	uint32_t VerID;         // DB Version ID used for this Session/Statement
	uint32_t StepID;        // Internal Primitive Sequence number
	uint32_t UniqueID;      // Unique ID for DEC and BPP
	uint32_t Priority;      // Priority level of the user
};

#if 0
struct AckNackHeader
{
	int VerID:16;
};

struct DiskResultsHeader
{
       uint64_t    LBID;
       uint64_t    VerID;
       uint64_t    ArbIndex;
       uint64_t    ISM;
       uint64_t    Status;
       uint8_t     data[DATA_BLOCK_SIZE];
};
#endif

//      COL_LOOPBACK

struct ColLoopback
{
	PrimitiveHeader Hdr;    // 64 bit header
};

//      COL_BY_SCAN
//Tied to ColByScanRangeRequestHeader and NewColRequestHeader.  Check other headers if modifying.

struct ColByScanRequestHeader
{
	PrimitiveHeader Hdr;    // 64 bit header
	uint64_t LBID;
	int32_t CompType;
	uint16_t DataSize;
	uint8_t DataType;       // enum ColDataType defined in calpont system catalog header file
	uint8_t OutputType;     // 1 = RID, 2 = Token, 3 = Both
	uint8_t BOP;            // 0 = N/A, 1 = AND, 2 = OR
	uint8_t RidFlags;		// a bitmap indicating the rid ranges in the resultM SB => row 7168-8191
	uint16_t NOPS;
	uint16_t NVALS;
	uint8_t sort;
};

//      COL_BY_SCAN_RANGE
//Tied to ColByScanRequestHeader and NewColRequestHeader.  Check other headers if modifying.

struct ColByScanRangeRequestHeader
{
	PrimitiveHeader Hdr;    // 64 bit header
	uint64_t LBID;		    // starting LBID
	int32_t CompType;
	uint16_t DataSize;
	uint8_t DataType;       // enum ColDataType defined in calpont system catalog header file
	uint8_t OutputType;     // 1 = RID, 2 = Token, 3 = Both
	uint8_t BOP;            // 0 = N/A, 1 = AND, 2 = OR
	uint8_t RidFlags;		// a bitmap indicating the rid ranges in the result MSB => row 7168-8191
	uint16_t NOPS;
	uint16_t NVALS;
	uint8_t sort;
	uint16_t Count;			//Number of LBID's
};

//      COL_BY_RID

struct ColByRIDRequestHeader
{
	PrimitiveHeader Hdr;                      // 64 bit header
	uint64_t LBID;
	int32_t CompType;
	uint16_t DataSize;
	uint8_t DataType;                            // enum ColDataType defined in calpont system catalog header file
	uint8_t OutputType;                          // 1 = RID, 2 = Token, 3 = Both
	uint8_t BOP;                                 // 0 = N/A, 1 = AND, 2 = OR
	uint8_t InputFlags;		// 1 = interpret each NOP & RID as a pair
	uint16_t NOPS;
	uint16_t NVALS;
	uint8_t sort;
};

//      COL_AGG_BY_SCAN

struct ColAggByScanRequestHeader
{
	PrimitiveHeader Hdr;                      // 64 bit header
	uint64_t LBID;
	uint16_t DataSize;
	uint8_t DataType;                            // enum ColDataType defined in calpont system catalog header file
	uint8_t OutputType;                          // 1 = RID, 2 = Token, 3 = Both
	uint8_t BOP;                                 // 0 = N/A, 1 = AND, 2 = OR
	uint8_t   ExtraNotUsed;
	uint16_t NOPS;
	uint16_t NVALS;
};

//      COL_AGG_BY_RID

struct ColAggByRIDRequestHeader
{
	PrimitiveHeader Hdr;                      // 64 bit header
	uint64_t LBID;
	int32_t CompType;
	uint16_t DataSize;
	uint8_t DataType;                            // enum ColDataType defined in calpont system catalog header file
	uint8_t OutputType;                          // 1 = RID, 2 = Token, 3 = Both
	uint8_t BOP;                                 // 0 = N/A, 1 = AND, 2 = OR
	uint8_t   ExtraNotUsed;
	uint16_t NOPS;
	uint16_t NVALS;
};

//      Loopback Results

struct LoopbackResultHeader
{
	PrimitiveHeader Hdr;
};

//      Column Results

struct ColResultHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint8_t RidFlags;
	uint16_t NVALS;
	uint16_t ValidMinMax; 			  // 1 if Min/Max are valid, otherwise 0
	uint32_t OutputType;
	int64_t Min; 				  // Minimum value in this block (signed)
	int64_t Max; 				  // Maximum value in this block (signed)
	uint32_t CacheIO;				  // I/O count from buffer cache
	uint32_t PhysicalIO;			  // Physical I/O count from disk
};

//      Column Aggregate results

struct ColAggResultHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint64_t MIN;                                // Minimum value in this block (signed)
	uint64_t MAX;                                // Maximum value in this block (signed)
	uint64_t SUM;                                // Sum of values in this block (unsigned)
	uint32_t SUMOverflow;                        // Overflow of sum (unsigned)
	uint16_t NVALS;                              // Number of values in this block
	uint16_t Pad1;
};

//      INDEX_BY_SCAN

struct IndexByScanRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint32_t State;
	uint8_t  Flags;
	uint8_t  DataSize;
	uint8_t DataType;                            // enum ColDataType defined in calpont system catalog header file
	uint8_t   ExtraNotUsed;
	uint16_t NVALS;
};

//      INDEX_BY_COMPARE

struct IndexByCompareRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint32_t State;
	uint8_t  Flags;
	uint8_t  DataSize;
	uint8_t DataType;                            // enum ColDataType defined in calpont system catalog header file
	uint8_t   ExtraNotUsed;
	uint16_t NVALS;
};

struct IndexResultHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint32_t State;
	uint16_t NVALS;
	uint16_t Pad;
};

//  p_IdxWalk

// this is used as input & output to the software p_IdxWalk processor.  Ideally
// there would only be one copy of the ISM and packet headers per returned result.

#ifdef __cplusplus
struct IndexWalkHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader Hdr;
	uint64_t SearchString[2];
	const std::vector<uint64_t> *SearchStrings;  //used only if NVALS > 2
	uint8_t SSlen;                               // width of the search argument in BITS
	uint8_t Shift;                                // initialize to zero when first sending to primitive
	uint8_t BOP;
	uint8_t COP1;
	uint8_t COP2;
	uint8_t State;                               //right now this is only 1 or 0, specifying entire subtrees
	uint16_t NVALS;
	uint64_t LBID:36;
	uint8_t SubBlock:5;
	uint8_t SBEntry:5;
};
#endif

// p_IdxList

struct IndexListHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
	// As the input parameter, what follows is IndexListParam[NVALS]
	// As the output parameter, what follows is IndexListResult[NVALS]
};

// p_IdxList parameter
struct IndexListParam
{
	uint64_t type  : 3;                          // 0 - header, 4 - subblock, 5 - block
	uint64_t spare : 15;
	uint64_t fbo   : 36;
	uint64_t sbid  : 5;
	uint64_t entry : 5;
// 		int64_t listValue;
};

struct IndexListEntry
{
	uint64_t type  : 3;
	uint64_t spare : 5;
	uint64_t ridCt : 10;
	uint64_t value : 46;
};

struct IndexListResult
{
	IndexListEntry entry;
	int64_t listValue;
};

enum IndexListType
{
	LIST_SIZE      = 0,
	EMPTY_LIST_PTR = 1,
	EMPTY_PTR      = 2,
	RID            = 3,
	LLP_SUBBLK     = 4,
	LLP_BLK        = 5,
	PARENT		   = 6,
	NOT_IN_USE     = 7
};

//      DICT_TOKEN_BY_INDEX_COMPARE
struct DictTokenByIndexRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
};

//      DICT_TOKEN_BY_SCAN_COMPARE

struct DataValue
{
	uint16_t len;
	char data[];
};

struct PrimToken
{
	uint64_t LBID;
	uint16_t offset;                             // measured in bytes
	uint16_t len;                                // # of bytes
};

// Masks for the flags member of TokenByScanRequestHeader
#define HAS_EQ_FILTER 0x1
#define IS_SYSCAT 0x2

struct TokenByScanRequestHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader Hdr;
	uint64_t LBID;
	int32_t CompType;
	uint8_t COP1;
	uint8_t COP2;
	uint8_t BOP;
	uint8_t OutputType;
	uint16_t NVALS;
	uint16_t flags;
	uint32_t Pad2;
	uint16_t Count;
};                     // what follows is NVALS DataValues.

// compatibility with Ron's stuff.
typedef TokenByScanRequestHeader DictTokenByScanRequestHeader;

struct TokenByScanResultHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader Hdr;
	uint32_t NBYTES;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t CacheIO;				// I/O count from buffer cache
	uint32_t PhysicalIO;			// Physical I/O count from disk
};                      // what follows is NVALS Tokens or DataValues.

//      DICT_SIGNATURE

struct DictSignatureRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	int32_t CompType;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
};
//Tied to DictSignatureRequestHeader, note if modifying either.
struct DictSignatureRangeRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	int32_t CompType;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
	uint16_t Count;
};


//      DICT_AGGREGATE

struct DictAggregateRequestHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	int32_t CompType;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
};

struct DictResultHeader
{
	PrimitiveHeader Hdr;
	uint64_t LBID;
	uint16_t NVALS;
	uint16_t Pad1;
	uint32_t Pad2;
};

struct AggregateSignatureRequestHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint16_t NVALS;
	PrimToken tokens[];
};

struct AggregateSignatureResultHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint16_t Count;
	// these implicitly follow the header
	// DataValue min;
	// DataValue max;
};

/* An array of these structures defines a filter applied by p_Col.  The length
 * of the val field should be the width of the column the filter will be applied
 * to, ex: 4 bytes long for a 32-bit column.
 */
struct ColArgs
{
	uint8_t COP;
	uint8_t rf;     // rounding flag: indicates if val is truncated or saturated
	                // for further evaluation of an equal condiction
	int8_t val[];
};

// const for rf
const uint8_t ROUND_POS = 0x01;  // actual value larger/longer than the stored value
const uint8_t ROUND_NEG = 0x80;  // actual value less than the stored value

//Tied to ColByScanRequestHeader and ColByScanRangeRequestHeader.  Check other headers if modifying.
struct NewColRequestHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint64_t LBID;
	int32_t CompType;
	uint16_t DataSize;
	uint8_t DataType;
	uint8_t OutputType;		// OT_DATAVALUE, OT_RID, or OT_BOTH
	uint8_t BOP;
// 	uint8_t InputFlags;		// 1 = interpret each NOP & RID as a pair (deprecated)
	uint8_t RidFlags;		// a bitmap indicating the rid ranges in the result MSB => row 7168-8191
	uint16_t NOPS;
	uint16_t NVALS;
	uint8_t sort;				//1 to sort
	// this follows the header
	// ColArgs ArgList[NOPS] (where the val field is DataSize bytes long)
	// uint16_t Rids[NVALS]  (each rid is relative to the given block)
};

struct NewColAggRequestHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint64_t LBID;
	int32_t CompType;
	uint8_t DataSize;
	uint8_t DataType;
	uint8_t OutputType;
	uint8_t BOP;
	uint8_t ExtraNotUsed;
	uint16_t NOPS;
	uint16_t NVALS;
	// this follows the header
	// ColArgs ArgList[NOPS] (where the val field is DataSize bytes long)
	// uint16_t Rids[NVALS] (each rid is relative to the given block)
};

struct NewColResultHeader
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint64_t LBID;
	uint8_t RidFlags;
	uint16_t NVALS;
	uint16_t ValidMinMax;		// 1 if Min/Max are valid, otherwise 0
	uint32_t OutputType;
	int64_t Min; 			    // Minimum value in this block for signed data types
	int64_t Max; 			    // Maximum value in this block for signed data types
	uint32_t CacheIO;			// I/O count from buffer cache
	uint32_t PhysicalIO;		// Physical I/O count from disk
	// if OutputType was OT_DATAVALUE, what follows is DataType[NVALS]
	// if OutputType was OT_RID, what follows is uint16_t Rids[NVALS]
	// if OutputType was OT_BOTH, what follows is NVALS <Rid, DataType> pairs
};

/* additional types to support p_dictionary */
struct DictFilterElement
{
	uint8_t COP;
	uint16_t len;  // this is the length of data, not the size of the entire entry
	uint8_t data[];
};

struct DictInput
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint64_t LBID;
	uint8_t BOP;
	uint8_t InputFlags;		// 1 -> 64-bit RID, 64-bit token pairs (old p_GetSignature behavior),
							// 0-> new behavior
	uint8_t OutputType;
	uint16_t NOPS;
	uint16_t NVALS;
	PrimToken tokens[];   // NVALS of these.
	// DictFilterElement[NOPS] filter;
};

struct DictAggregate
	{
	uint16_t Count;
	// DataValue min;
	// DataValue max;
};

struct DictOutput
{
	ISMPacketHeader ism;
	PrimitiveHeader hdr;
	uint64_t LBID;
	uint16_t NVALS;
	uint16_t Pad;
	uint32_t NBYTES;
	uint32_t CacheIO;				// I/O count from buffer cache
	uint32_t PhysicalIO;			// Physical I/O count from disk
	// What follows this header depends on OutputType.
	// for each NVAL, what follows is ...
	//   if OutputType | OT_RID & InputFlags==1, the 64-bit RID associated with the input token
	//   if OutputType | OT_TOKEN, a PrimToken
	//   if OutputType | OT_DATAVALUE, a DataValue containing the string in the dict block
	//   if OutputType | OT_INPUT, a DataValue containing the first filter string...
	//        ... that matched (only makes sense when BOP is OR).
	// DictAggregate agg;  (if OutputType | OT_AGGREGATE)
};		//same as TokenByScanResultHeader at the moment

struct OldGetSigParams {
	uint64_t rid;
	uint16_t offsetIndex;
};

struct LbidAtVer
{
	uint64_t LBID;
	uint32_t Ver;
};

#ifdef _MSC_VER
#pragma warning (pop)
#endif

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif //JOBLIST_PRIMITIVE_H
// vim:ts=4 sw=4:

