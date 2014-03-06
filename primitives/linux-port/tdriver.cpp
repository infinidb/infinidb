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

/*********************************************************************
 *   $Id: tdriver.cpp 141 2006-11-09 18:00:25Z pleblanc $
 *
 ********************************************************************/

/** @file
 * class MyClass Interface
 */

#include <iostream>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdexcept>
#include <signal.h>
#include <values.h>
#include <cppunit/extensions/HelperMacros.h>

#include "primitiveprocessor.h"

using namespace std;

int done;

void alarm_handler(int sig) {
	done = 1;
}

class PrimTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(PrimTest);

CPPUNIT_TEST(p_IdxWalk_1_eq_1);
CPPUNIT_TEST(p_IdxWalk_1_eq_2);
CPPUNIT_TEST(p_IdxWalk_1_lt_1);
CPPUNIT_TEST(p_IdxWalk_1_lte_1);
CPPUNIT_TEST(p_IdxWalk_1_gt_1);
CPPUNIT_TEST(p_IdxWalk_1_gte_1);
CPPUNIT_TEST(p_IdxWalk_1_neq_1);
CPPUNIT_TEST(p_IdxWalk_2_eq_or_1);
CPPUNIT_TEST(p_IdxWalk_2_range_1);
CPPUNIT_TEST(p_IdxWalk_2_range_2);
CPPUNIT_TEST(p_IdxWalk_2_range_3);
CPPUNIT_TEST(p_IdxWalk_40_eq_or_1);
CPPUNIT_TEST(p_IdxWalk_40_neq_and_1);

CPPUNIT_TEST(p_AggregateSignature_1);
CPPUNIT_TEST(p_AggregateSignature_2);

CPPUNIT_TEST(p_TokenByScan_1);
CPPUNIT_TEST(p_TokenByScan_2);
CPPUNIT_TEST(p_TokenByScan_3);
CPPUNIT_TEST(p_TokenByScan_gt_1);
CPPUNIT_TEST(p_TokenByScan_gte_1);
CPPUNIT_TEST(p_TokenByScan_lt_1);
CPPUNIT_TEST(p_TokenByScan_lte_1);
CPPUNIT_TEST(p_TokenByScan_neq_1);
CPPUNIT_TEST(p_TokenByScan_range_1);
CPPUNIT_TEST(p_TokenByScan_eq_6);
CPPUNIT_TEST(p_TokenByScan_neq_6);
CPPUNIT_TEST(p_TokenByScan_token_eq_1);
CPPUNIT_TEST(p_TokenByScan_like_1);

CPPUNIT_TEST(p_IdxList_1);
CPPUNIT_TEST(p_IdxList_2);

// whole block tests
CPPUNIT_TEST(p_Col_1);
CPPUNIT_TEST(p_Col_2);
CPPUNIT_TEST(p_Col_3);
CPPUNIT_TEST(p_Col_4);

// rid array test
CPPUNIT_TEST(p_Col_5);

// whole block range tests
CPPUNIT_TEST(p_Col_6);
CPPUNIT_TEST(p_Col_7);
CPPUNIT_TEST(p_Col_8);

// rid array range test
CPPUNIT_TEST(p_Col_9);

// OT_RID output type tests
CPPUNIT_TEST(p_Col_10);

// OT_BOTH output type tests
CPPUNIT_TEST(p_Col_11);

// 8-bit null value test
CPPUNIT_TEST(p_Col_12);

// alternating NOP, RID pairs on input
CPPUNIT_TEST(p_Col_13);

// double column test
CPPUNIT_TEST(p_Col_double_1);

// float column test
CPPUNIT_TEST(p_Col_float_1);

// negative float column test
CPPUNIT_TEST(p_Col_neg_float_1);

// negative double column test
CPPUNIT_TEST(p_Col_neg_double_1);

// some ports of TokenByScan tests to validate similar & shared code
CPPUNIT_TEST(p_Dictionary_1);
CPPUNIT_TEST(p_Dictionary_2);
CPPUNIT_TEST(p_Dictionary_3);
CPPUNIT_TEST(p_Dictionary_gt_1);

// exercise the OT_TOKEN & OT_INPUTARG output code
CPPUNIT_TEST(p_Dictionary_token_1);
CPPUNIT_TEST(p_Dictionary_inputArg_1);

// add the OT_AGGREGATE output flag
CPPUNIT_TEST(p_Dictionary_token_agg_1);

// restrict the scan to a list of tokens
CPPUNIT_TEST(p_Dictionary_inToken_1);

// test the old GetSignature behavior
//CPPUNIT_TEST(p_Dictionary_oldgetsig_1);

// test & benchmark the new LIKE operator
CPPUNIT_TEST(p_Dictionary_like_1);
CPPUNIT_TEST(p_Dictionary_like_2);
CPPUNIT_TEST(p_Dictionary_like_3);
CPPUNIT_TEST(p_Dictionary_like_4);

// new LIKE-regexp functionality
CPPUNIT_TEST(p_Dictionary_like_5);	// "_NDO%a%"
CPPUNIT_TEST(p_Dictionary_like_6);	// "%NIT%ING%D%"
CPPUNIT_TEST(p_Dictionary_like_7);	// "UNI%TES"
CPPUNIT_TEST(p_Dictionary_like_8);	// "%TH_OP%"

// CPPUNIT_TEST(p_Dictionary_like_prefixbench_1);
// CPPUNIT_TEST(p_Dictionary_like_substrbench_1);

CPPUNIT_TEST_SUITE_END();

private:
public:

void p_IdxWalk_1_eq_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	// this search should end at FBO 10251 subblock 22 entry 12.
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_EQ;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	while (params->LBID != 10251) {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		CPPUNIT_ASSERT(results.size() == 1);
		params = results.at(0);
		results.clear();
	}
	delete params;
	close(fd);
}

void p_IdxWalk_1_lt_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_LT;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}		

void p_IdxWalk_1_lte_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_LE;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}		

void p_IdxWalk_1_gt_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_GT;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}		

void p_IdxWalk_1_gte_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_GE;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}

void p_IdxWalk_1_neq_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_NE;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}

void p_IdxWalk_1_eq_2() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	// this shouldn't match anywhere
	searchKey = (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_EQ;
	params->NVALS = 1;
	params->SubBlock = 1;
	params->SSlen = 14;

	pp.setBlockPtr((int *) block);

	lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
	err = read(fd, block, BLOCK_SIZE);
	if (err <= 0) {
		cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
		throw runtime_error("p_IdxWalk: Couldn't read the file");
	}
	if (err != BLOCK_SIZE) {
		cerr << "p_IdxWalk: could not read a whole block" << endl;
		throw runtime_error("p_IdxWalk: could not read a whole block");
	}

	pp.p_IdxWalk(params, &results);
	delete params;
	CPPUNIT_ASSERT(results.size() == 0);
	results.clear();
	
	close(fd);
}	

void p_IdxWalk_2_eq_or_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey[2];
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey[0] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	searchKey[1] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 0;
		
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey[0];
	params->SearchString[1] = searchKey[1];
	params->COP1 = COMPARE_EQ;
	params->COP2 = COMPARE_EQ;
	params->BOP = BOP_OR;
	params->NVALS = 2;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);
	CPPUNIT_ASSERT(results.size() == 1);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}	

// this test singles out the same entry we've used above
void p_IdxWalk_2_range_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey[2];
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey[0] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 2;
	searchKey[1] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 0;
		
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey[0];
	params->SearchString[1] = searchKey[1];
	params->COP1 = COMPARE_LT;
	params->COP2 = COMPARE_GT;
	params->BOP = BOP_AND;
	params->NVALS = 2;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);
 	CPPUNIT_ASSERT(results.size() == 1);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}	

// this test singles out a contiguous chunk of 3 entries
void p_IdxWalk_2_range_2() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey[2];
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey[0] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 3;
	searchKey[1] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
		
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey[0];
	params->SearchString[1] = searchKey[1];
	params->COP1 = COMPARE_LE;
	params->COP2 = COMPARE_GE;
	params->BOP = BOP_AND;
	params->NVALS = 2;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}	

// this test excludes all entries.
void p_IdxWalk_2_range_3() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey[2];
	int fd;
	uint32_t err;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey[0] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 3;
	searchKey[1] = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
		
	params = new IndexWalkHeader();

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey[0];
	params->SearchString[1] = searchKey[1];
	params->COP1 = COMPARE_GE;
	params->COP2 = COMPARE_LE;
	params->BOP = BOP_AND;
	params->NVALS = 2;
	params->SubBlock = 1;
	params->SSlen = 64;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	CPPUNIT_ASSERT(results.size() == 0);
	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}	

void p_IdxWalk_40_eq_or_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err, i;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;
	vector<Int64> *vTmp;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();		

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_EQ;
	params->BOP = BOP_OR;
	params->NVALS = 40;
	params->SubBlock = 1;
	params->SSlen = 64;
	vTmp = new vector<Int64>();
	for (i = 0; i < 40; i++)
		vTmp->push_back(searchKey+i);
	params->SearchStrings = vTmp;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
 		if (params->NVALS > 2 && params->State == 0)
 			delete params->SearchStrings;
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}	

void p_IdxWalk_40_neq_and_1() 
{
	PrimitiveProcessor pp;
	char block[BLOCK_SIZE];
	u_int64_t searchKey;
	int fd;
	uint32_t err, i;
	string filename("FILE_990.dat");
	IndexWalkHeader *params;
	vector<IndexWalkHeader *> results;
	vector<IndexWalkHeader *>::iterator it;
	vector<Int64> *vTmp;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxWalk: skipping this test; needs the index tree file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}
	
	searchKey = (12LL << 59) | (21LL << 54) | (17LL << 49) | (22LL << 44) | 
		(6LL << 39) | (29LL << 34) | (10LL << 29) | (19LL << 24) |
		(6LL << 19) | (0LL << 14) | (24LL << 9) | (3LL << 4) | 1;
	
	params = new IndexWalkHeader();		

	memset(params, 0, sizeof(IndexWalkHeader));
	params->SearchString[0] = searchKey;
	params->COP1 = COMPARE_NE;
	params->BOP = BOP_AND;
	params->NVALS = 40;
	params->SubBlock = 1;
	params->SSlen = 64;
	vTmp = new vector<Int64>();
	for (i = 0; i < 40; i++)
		vTmp->push_back(searchKey+i);
	params->SearchStrings = vTmp;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->LBID * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}

		pp.p_IdxWalk(params, &results);
  		if (params->NVALS > 2 && params->State == 0)
  			delete params->SearchStrings;
		delete params;
		params = NULL;
		for (it = results.begin(); it != results.end(); it++)
			if ((*it)->Shift < (*it)->SSlen) {
				params = *it;
				results.erase(it);
				break;
			}
	} while (params != NULL);

	// No automatic way to verify the results yet.  Need a field that says what
	// value was matched so we can verify they're all < the search key.	

	for (it = results.begin(); it != results.end(); it++)
		delete *it;
	
	close(fd);
}

// this test scans the entire block
void p_AggregateSignature_1()
{

	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	AggregateSignatureRequestHeader *cmd;
	AggregateSignatureResultHeader *results;
	DataValue *dvPtr;
	char minmax[BLOCK_SIZE];
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<AggregateSignatureRequestHeader *>(input);
	results = reinterpret_cast<AggregateSignatureResultHeader *>(output);
	cmd->NVALS = 0;

	pp.setBlockPtr((int *) block);
	pp.p_AggregateSignature(cmd, results, BLOCK_SIZE, &err);
	dvPtr = reinterpret_cast<DataValue *>(&output[sizeof(AggregateSignatureResultHeader)]);
	memcpy(minmax, dvPtr->data, dvPtr->len);
	minmax[dvPtr->len] = '\0';
	CPPUNIT_ASSERT(results->Count == 50);
	CPPUNIT_ASSERT(strcmp(minmax, "ALGERIA") == 0);
// 	cerr << "count is " << results->Count << endl;
// 	cerr << "min is " << minmax << endl;
	dvPtr = reinterpret_cast<DataValue *>
		(&output[sizeof(AggregateSignatureResultHeader) + dvPtr->len + sizeof(DataValue)]);
	memcpy(minmax, dvPtr->data, dvPtr->len);
	minmax[dvPtr->len] = '\0';
 	CPPUNIT_ASSERT(strcmp(minmax, "XUSSIA") == 0);
// 	cerr << "max is " << minmax << endl;
}

// this test scans a set of tokens
void p_AggregateSignature_2()
{

	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	AggregateSignatureRequestHeader *cmd;
	AggregateSignatureResultHeader *results;
	DataValue *dvPtr;
	char minmax[BLOCK_SIZE];
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<AggregateSignatureRequestHeader *>(input);
	results = reinterpret_cast<AggregateSignatureResultHeader *>(output);
	cmd->NVALS = 4;
	// JORDAN
	cmd->tokens[0].LBID = 0;
	cmd->tokens[0].offset = 7928;
	cmd->tokens[0].len = 6;
	// XHINA
	cmd->tokens[1].LBID = 0;
	cmd->tokens[1].offset = 8074;
	cmd->tokens[1].len = 5;
	// ARGENTINA
	cmd->tokens[2].LBID = 0;
	cmd->tokens[2].offset = 7999;
	cmd->tokens[2].len = 9;
	// UNITED STATES
	cmd->tokens[3].LBID = 0;
	cmd->tokens[3].offset = 7838;
	cmd->tokens[3].len = 13;

	pp.setBlockPtr((int *) block);
	pp.p_AggregateSignature(cmd, results, BLOCK_SIZE, &err);
	
	dvPtr = reinterpret_cast<DataValue *>(&output[sizeof(AggregateSignatureResultHeader)]);
	memcpy(minmax, dvPtr->data, dvPtr->len);
	minmax[dvPtr->len] = '\0';
 	CPPUNIT_ASSERT(results->Count == 4);
 	CPPUNIT_ASSERT(strcmp(minmax, "ARGENTINA") == 0);
//  	cerr << "count is " << results->Count << endl;
//  	cerr << "min is " << minmax << endl;
	dvPtr = reinterpret_cast<DataValue *>
		(&output[sizeof(AggregateSignatureResultHeader) + dvPtr->len + sizeof(DataValue)]);
	memcpy(minmax, dvPtr->data, dvPtr->len);
	minmax[dvPtr->len] = '\0';
  	CPPUNIT_ASSERT(strcmp(minmax, "XHINA") == 0);
//  	cerr << "max is " << minmax << endl;
}

void p_TokenByScan_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_EQ;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 5;
	strncpy(args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
	args = reinterpret_cast<DataValue *>(&results[1]);
	args->data[args->len] = '\0';		// not reusable in tests with multiple matches.
 	CPPUNIT_ASSERT(results->NVALS == 1);
 	CPPUNIT_ASSERT(args->len == 5);
 	CPPUNIT_ASSERT(strncmp(args->data, "XHINA", 5) == 0);
}

void p_TokenByScan_2()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd;	
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 2;
	cmd->BOP = BOP_OR;
	cmd->COP1 = COMPARE_EQ;
	cmd->COP2 = COMPARE_EQ;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 13;
	strncpy(args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DataValue *>(&input[sizeof(TokenByScanRequestHeader) + args->len + 
		sizeof(DataValue)]);
	args->len = 5;
	strncpy(args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
	args = reinterpret_cast<DataValue *>(&results[1]);
	memcpy(tmp, args->data, args->len);
	tmp[args->len] = '\0';
 	CPPUNIT_ASSERT(results->NVALS == 2);
// 	cout << "len is " << args->len << endl;
// 	cout << "data is " << tmp << endl;
	
	args = reinterpret_cast<DataValue *>(&output[sizeof(TokenByScanResultHeader) + args->len + 
		sizeof(DataValue)]);
	memcpy(tmp, args->data, args->len);
	tmp[args->len] = '\0';
// 	cout << "len is " << args->len << endl;
// 	cout << "data is " << tmp << endl;
}

void p_TokenByScan_3()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 0;
	cmd->COP1 = COMPARE_EQ;
	cmd->OutputType = OT_DATAVALUE;

	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
	argsOffset = sizeof(TokenByScanResultHeader);
// 	cout << "NVALS = " << results->NVALS << endl;
 	CPPUNIT_ASSERT(results->NVALS == 50);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
//  		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}
}


void p_TokenByScan_gt_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_GT;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 7;
	strncpy(args->data, "GERMANY", 7);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 41);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_gte_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_GE;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 7;
	strncpy(args->data, "GERMANY", 7);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 42);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_lt_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_LT;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 7;
	strncpy(args->data, "GERMANY", 7);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 8);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_lte_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_LE;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 7;
	strncpy(args->data, "GERMANY", 7);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 9);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_neq_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_NE;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 7;
	strncpy(args->data, "GERMANY", 7);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 49);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_range_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);

	argsOffset = sizeof(TokenByScanRequestHeader);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);

	cmd->NVALS = 2;
	cmd->COP1 = COMPARE_LT;
	cmd->COP2 = COMPARE_GT;
	cmd->BOP = BOP_AND;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 5;
	strncpy(args->data, "KENYA", 5);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 6;
	strncpy(args->data, "BRAZIL", 6);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 12);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_eq_6()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);

	argsOffset = sizeof(TokenByScanRequestHeader);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);

	cmd->NVALS = 6;
	cmd->COP1 = COMPARE_EQ;
	cmd->BOP = BOP_OR;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 5;
	strncpy(args->data, "KENYA", 5);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 6;
	strncpy(args->data, "BRAZIL", 6);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 8;
	strncpy(args->data, "ETHIOPIA", 8);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 6;
	strncpy(args->data, "CANADA", 6);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 13;
	strncpy(args->data, "UNITED_STATES", 13);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 13;
	strncpy(args->data, "UNITED STATES", 13);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
// 	cout << "NVALS = " << results->NVALS << endl;
	CPPUNIT_ASSERT(results->NVALS == 5);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
// 		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_neq_6()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);

	argsOffset = sizeof(TokenByScanRequestHeader);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);

	cmd->NVALS = 6;
	cmd->COP1 = COMPARE_NE;
	cmd->BOP = BOP_AND;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 5;
	strncpy(args->data, "KENYA", 5);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 6;
	strncpy(args->data, "BRAZIL", 6);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 8;
	strncpy(args->data, "ETHIOPIA", 8);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 6;
	strncpy(args->data, "CANADA", 6);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 13;
	strncpy(args->data, "UNITED_STATES", 13);

	argsOffset += args->len + sizeof(DataValue);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);
	args->len = 13;
	strncpy(args->data, "UNITED STATES", 13);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
//  	cout << "NVALS = " << results->NVALS << endl;
 	CPPUNIT_ASSERT(results->NVALS == 45);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
//  		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_like_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE], tmp[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	int fd, argsOffset, i;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);

	argsOffset = sizeof(TokenByScanRequestHeader);
	args = reinterpret_cast<DataValue *>(&input[argsOffset]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_LIKE;
	cmd->BOP = BOP_AND;
	cmd->OutputType = OT_DATAVALUE;
	args->len = 4;
	strncpy(args->data, "%NYA", 4);
	
	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
//   	cout << "NVALS = " << results->NVALS << endl;
  	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(TokenByScanResultHeader);
	for (i = 0; i < results->NVALS; i++) {
		args = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, args->data, args->len);
		tmp[args->len] = '\0';
//   		cout << "  " << i << ": len=" << args->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + args->len;
	}

}

void p_TokenByScan_token_eq_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	TokenByScanRequestHeader *cmd;
	TokenByScanResultHeader *results;
	DataValue *args;
	PrimToken *result;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<TokenByScanRequestHeader *>(input);
	results = reinterpret_cast<TokenByScanResultHeader *>(output);
	args = reinterpret_cast<DataValue *>(&cmd[1]);

	cmd->NVALS = 1;
	cmd->COP1 = COMPARE_EQ;
	cmd->OutputType = OT_TOKEN;
	cmd->LBID = 13;  		// arbitrary #
	args->len = 5;
	strncpy(args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_TokenByScan(cmd, results, BLOCK_SIZE);
	
	result = reinterpret_cast<PrimToken *>(&results[1]);
 	CPPUNIT_ASSERT(results->NVALS == 1);
 	CPPUNIT_ASSERT(result->len == 5);
 	CPPUNIT_ASSERT(result->offset == 19);
	CPPUNIT_ASSERT(result->LBID == 13);
}

void p_IdxList_1()
{

	// there should be a list header (corresponds to the search value we used in
	// p_IdxWalk) at FBO 10251 subblock 22 entry 12.  The first LBID of the
	// index list file is 10000

	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], input[BLOCK_SIZE], output[BLOCK_SIZE];
	string filename("FILE_991.dat");
	IndexListHeader *hdr, *resultHdr;
	IndexListParam *params;
	IndexListEntry *results;
	vector<IndexListEntry> bigResults;
	vector<IndexListEntry>::iterator it;
	int fd;
	uint32_t err, i;
	bool continuationPtr;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxList: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}

	hdr = reinterpret_cast<IndexListHeader *>(input);
	params = reinterpret_cast<IndexListParam *>(&input[sizeof(IndexListHeader)]);
	resultHdr = reinterpret_cast<IndexListHeader *>(output);
	results = reinterpret_cast<IndexListEntry *>(&output[sizeof(IndexListHeader)]);
	
	memset(hdr, 0, sizeof(IndexListHeader));
	hdr->NVALS = 1;
	
	params->fbo = 11;
	params->sbid = 22;
	params->entry = 12;
	params->type = LIST_SIZE;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->fbo * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}
		
		pp.p_IdxList(hdr, resultHdr, 0);
	
		// scan for a continuation pointer
		continuationPtr = false;
		for (i = 0; i < resultHdr->NVALS; i++) {
			if (results[i].type == LLP_SUBBLK || results[i].type == LLP_BLK) {
				// the header for the next iteration can stay the same, the
				// argument has to be the LLP entry.
				memcpy(params, &results[i], sizeof(IndexListEntry));
				CPPUNIT_ASSERT(params->fbo >= 20480);
				CPPUNIT_ASSERT(params->fbo < 30720);
				params->fbo -= 20480;  // this happens to be the base LBID for the test data
				continuationPtr = true;
			}
			else {
				CPPUNIT_ASSERT(results[i].type == RID);
				bigResults.push_back(results[i]);
			}
		}
/*
		// this clause doesn't get used in this test b/c there is only one RID	
		if (results->type == LLP_SUBBLK || results->type == LLP_BLK) {
			CPPUNIT_ASSERT(resultHdr->NVALS > 1);

			// the header for the next iteration can stay the same, the
			// argument has to be the LLP entry.
			memcpy(params, results, sizeof(IndexListEntry));
			// store the results returned so far
			for (i = 1; i < resultHdr->NVALS; i++)
				bigResults.push_back(results[i]);
		}
		else
			for (i = 0; i < resultHdr->NVALS; i++)
				bigResults.push_back(results[i]);
*/
	} while (continuationPtr);
	
	CPPUNIT_ASSERT(bigResults.size() == 1);
// 	cout << endl << "RID count: " << bigResults.size() << endl;
	for (i = 1, it = bigResults.begin(); it != bigResults.end(); it++, i++) {
// 		cout << "  " << i << ": type=" << (*it).type << " rid=" << (*it).value << endl;
		CPPUNIT_ASSERT((*it).type == RID);
		CPPUNIT_ASSERT((*it).value == 100);
	}
}

void p_IdxList_2()
{

	// there should be a list header (corresponds to the search value we used in
	// p_IdxWalk) at FBO 10251 subblock 22 entry 12.  The first LBID of the
	// index list file is 10000

	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], input[BLOCK_SIZE], output[BLOCK_SIZE];
	string filename("FILE233.cdf");
	IndexListHeader *hdr, *resultHdr;
	IndexListParam *params;
	IndexListEntry *results;
	vector<IndexListEntry> bigResults;
	vector<IndexListEntry>::iterator it;
	int fd;
	uint32_t err, i;
	bool continuationPtr;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_IdxList: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_IdxWalk: file not found...");
		return;
	}

	hdr = reinterpret_cast<IndexListHeader *>(input);
	params = reinterpret_cast<IndexListParam *>(&input[sizeof(IndexListHeader)]);
	resultHdr = reinterpret_cast<IndexListHeader *>(output);
	results = reinterpret_cast<IndexListEntry *>(&output[sizeof(IndexListHeader)]);
	
	memset(hdr, 0, sizeof(IndexListHeader));
	hdr->NVALS = 1;
	
	params->fbo = 11;
	params->sbid = 10;
	params->entry = 28;
	params->type = LIST_SIZE;

	pp.setBlockPtr((int *) block);

	do {
		lseek(fd, params->fbo * BLOCK_SIZE, SEEK_SET);
		err = read(fd, block, BLOCK_SIZE);
		if (err <= 0) {
			cerr << "p_IdxWalk: Couldn't read the file " << filename << endl;
			throw runtime_error("p_IdxWalk: Couldn't read the file");
		}
		if (err != BLOCK_SIZE) {
			cerr << "p_IdxWalk: could not read a whole block" << endl;
			throw runtime_error("p_IdxWalk: could not read a whole block");
		}
		
		pp.p_IdxList(hdr, resultHdr, 0);
	
		// scan for a continuation pointer
		continuationPtr = false;
		for (i = 0; i < resultHdr->NVALS; i++) {
			if (results[i].type == LLP_SUBBLK || results[i].type == LLP_BLK) {
				// the header for the next iteration can stay the same, the
				// argument has to be the LLP entry.
				memcpy(params, &results[i], sizeof(IndexListEntry));
				CPPUNIT_ASSERT(params->fbo >= 20480);
				CPPUNIT_ASSERT(params->fbo < 30720);
				params->fbo -= 20480;  // this happens to be the base LBID for the test data
				continuationPtr = true;
			}
			else {
				CPPUNIT_ASSERT(results[i].type == RID);
				bigResults.push_back(results[i]);
			}
		}

/*
		if (results->type == LLP_SUBBLK || results->type == LLP_BLK) {
			CPPUNIT_ASSERT(resultHdr->NVALS > 1);

			// the header for the next iteration can stay the same, the
			// argument has to be the LLP entry.
			memcpy(params, results, sizeof(IndexListEntry));
			CPPUNIT_ASSERT(params->fbo >= 20480);
			CPPUNIT_ASSERT(params->fbo < 30720);
			params->fbo -= 20480;  // this happens to be the base LBID for the test data

			// store the results returned so far
			for (i = 1; i < resultHdr->NVALS; i++)
				bigResults.push_back(results[i]);
		}
		else
			for (i = 0; i < resultHdr->NVALS; i++)
				bigResults.push_back(results[i]);
*/
	} while (continuationPtr);
	
	CPPUNIT_ASSERT(bigResults.size() == 8901);
// 	cout << endl << "RID count: " << bigResults.size() << endl;
	for (i = 1, it = bigResults.begin(); it != bigResults.end(); it++, i++) {
//  		cout << "  " << i << ": type=" << (*it).type << " rid=" << (*it).value << endl;
 		CPPUNIT_ASSERT((*it).type == RID);
 		CPPUNIT_ASSERT((*it).value == (uint32_t) i-1);
	}
}

void p_Col_1() 
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	uint8_t *results;
	int fd;
	uint32_t i, written;
	string filename("col1block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_1: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_1: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_1: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_1: could not read a whole block" << endl;
		throw runtime_error("p_Col_1: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 1;
	in->DataType = CalpontSystemCatalog::CHAR;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 0;
	in->NVALS = 0;
	in->InputFlags = 0;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = &output[sizeof(NewColResultHeader)];
// 	cout << "NVALS = " << out->NVALS << endl;
	CPPUNIT_ASSERT(out->NVALS == 8160);
	for (i = 0; i < out->NVALS; i++) {
// 		cout << i << ": " << hex << (int)results[i] << dec << endl;
		CPPUNIT_ASSERT(results[i] == i % 255);
	}

	close(fd);
}

void p_Col_2()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	uint16_t *results;
	uint32_t written, i;
	int fd;
	string filename("col2block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_2: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_2: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_2: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_2: could not read a whole block" << endl;
		throw runtime_error("p_Col_2: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 2;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 0;
	in->NVALS = 0;
	in->InputFlags = 0;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<uint16_t *>(&output[sizeof(NewColResultHeader)]);
//  	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 4096);
	for (i = 0; i < out->NVALS; i++) {
//  		cout << i << ": " << hex << (int)results[i] << dec << endl;
 		CPPUNIT_ASSERT(results[i] == i);
	}

	close(fd);
}

void p_Col_3()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	uint32_t *results;
	uint32_t written, i;
	int fd;
	string filename("col4block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_3: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_3: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_2: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_3: could not read a whole block" << endl;
		throw runtime_error("p_Col_3: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 4;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 0;
	in->NVALS = 0;
	in->InputFlags = 0;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<uint32_t *>(&output[sizeof(NewColResultHeader)]);
//  	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 2048);
	for (i = 0; i < out->NVALS; i++) {
//  		cout << i << ": " << hex << (int)results[i] << dec << endl;
 		CPPUNIT_ASSERT(results[i] == (uint32_t) i);
	}

	close(fd);
}

void p_Col_4()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	u_int64_t *results;
	uint32_t written, i;
	int fd;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_4: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_4: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_4: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_4: could not read a whole block" << endl;
		throw runtime_error("p_Col_4: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 0;
	in->NVALS = 0;
	in->InputFlags = 0;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<u_int64_t *>(&output[sizeof(NewColResultHeader)]);
//   	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 1024);
	for (i = 0; i < out->NVALS; i++) {
//   		cout << i << ": " << hex << (int)results[i] << dec << endl;
 		CPPUNIT_ASSERT(results[i] == (uint32_t) i);
	}

	close(fd);
}

void p_Col_5()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	uint8_t *results;
	uint16_t *rids;
	uint32_t written, i;
	int fd;
	string filename("col1block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_5: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_5: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_5: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_5: could not read a whole block" << endl;
		throw runtime_error("p_Col_5: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	rids = reinterpret_cast<uint16_t *>(&in[1]);	

	in->DataSize = 1;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 0;
	in->NVALS = 2;
	in->InputFlags = 0;
	rids[0] = 20;
	rids[1] = 17;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<uint8_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
  	CPPUNIT_ASSERT(out->NVALS == 2);
	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
  		CPPUNIT_ASSERT(results[i] == rids[i]);
	}

	close(fd);
}

void p_Col_6()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	uint32_t *results;
	uint32_t written, i;
	int fd, tmp;
	string filename("col4block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_6: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_6: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_6: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_6: could not read a whole block" << endl;
		throw runtime_error("p_Col_6: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&in[1]);	

	in->DataSize = 4;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 2;
	in->BOP = BOP_AND;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 20;
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_GT;
	tmp = 10;
	memcpy(args->val, &tmp, in->DataSize);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<uint32_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
   	CPPUNIT_ASSERT(out->NVALS == 9);
	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
   		CPPUNIT_ASSERT(results[i] == 11 + (uint32_t)i);
	}

	close(fd);
}

void p_Col_7()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	u_int64_t *results;
	uint32_t written, i;
	int fd;
	int64_t tmp;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_7: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_7: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_7: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_7: could not read a whole block" << endl;
		throw runtime_error("p_Col_7: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 2;
	in->BOP = BOP_OR;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10;
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_GT;
	tmp = 1000;
	memcpy(args->val, &tmp, in->DataSize);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<u_int64_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
	CPPUNIT_ASSERT(out->NVALS == 33);
	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
     	CPPUNIT_ASSERT(results[i] == (uint32_t) (i < 10 ? i : i - 10 + 1001));
	}

	close(fd);
}

void p_Col_8()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	u_int64_t *results;
	uint32_t written, i;
	int fd;
	int64_t tmp;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_7: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_7: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_7: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_7: could not read a whole block" << endl;
		throw runtime_error("p_Col_7: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 2;
	in->BOP = BOP_OR;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10;
	args->COP = COMPARE_EQ;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_EQ;
	tmp = 1000;
	memcpy(args->val, &tmp, in->DataSize);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<u_int64_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 2);
	CPPUNIT_ASSERT(results[0] == 10);
	CPPUNIT_ASSERT(results[1] == 1000);
// 	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
//      	CPPUNIT_ASSERT(results[i] == (uint32_t) (i < 10 ? i : i - 10 + 1001));
// 	}

	close(fd);
}

void p_Col_9()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	uint16_t *rids;
	u_int64_t *results;
	uint32_t written, i;
	int fd;
	int64_t tmp;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_9: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_9: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_9: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_9: could not read a whole block" << endl;
		throw runtime_error("p_Col_9: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_DATAVALUE;
	in->NOPS = 2;
	in->BOP = BOP_OR;
	in->NVALS = 2;
	in->InputFlags = 0;

	tmp = 10;
	args->COP = COMPARE_EQ;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_EQ;
	tmp = 1000;
	memcpy(args->val, &tmp, in->DataSize);

	rids = reinterpret_cast<uint16_t *>(&input[sizeof(NewColRequestHeader) + 
	    2*(sizeof(ColArgs) + in->DataSize)]);

	rids[0] = 10;
	rids[1] = 100;

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<u_int64_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 1);
	CPPUNIT_ASSERT(results[0] == 10);
// 	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
//      	CPPUNIT_ASSERT(results[i] == (uint32_t) (i < 10 ? i : i - 10 + 1001));
// 	}

	close(fd);
}

void p_Col_10()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	int16_t *results;
	uint32_t written, i;
	int fd;
	int64_t tmp;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_10: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_10: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_10: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_10: could not read a whole block" << endl;
		throw runtime_error("p_Col_10: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_RID;
	in->NOPS = 2;
	in->BOP = BOP_OR;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10;
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_GT;
	tmp = 1000;
	memcpy(args->val, &tmp, in->DataSize);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<int16_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 33);
	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << hex << (int)results[i] << dec << endl;
      	CPPUNIT_ASSERT(results[i] == (i < 10 ? i : i - 10 + 1001));
	}

	close(fd);
}

void p_Col_11()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	int16_t *resultRid;
	int64_t *resultVal;
	uint32_t written, i;
	int fd;
	int64_t tmp;
	string filename("col8block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_11: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_11: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_11: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_11: could not read a whole block" << endl;
		throw runtime_error("p_Col_11: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_BOTH;
	in->NOPS = 2;
	in->BOP = BOP_OR;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10;
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, in->DataSize);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 
	    sizeof(ColArgs) + in->DataSize]);
	args->COP = COMPARE_GT;
	tmp = 1000;
	memcpy(args->val, &tmp, in->DataSize);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

//    	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 33);
	for (i = 0; i < out->NVALS; i++) {
		resultRid = reinterpret_cast<int16_t *>(&output[
			sizeof(NewColResultHeader) + i * (sizeof(Int16) + in->DataSize)]);
		resultVal = reinterpret_cast<int64_t *>(&resultRid[1]);

//    		cout << i << ":   rid:" << (int) *resultRid << " val:" << *resultVal << endl; 
      	CPPUNIT_ASSERT(*resultRid == (i < 10 ? i : i - 10 + 1001));
		CPPUNIT_ASSERT(*resultVal == (i < 10 ? i : i - 10 + 1001));
	}

	close(fd);
}

void p_Col_12() 
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	uint8_t *results;
	uint32_t written, i;
	int fd;
	string filename("col1block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_12: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_12: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_12: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_12: could not read a whole block" << endl;
		throw runtime_error("p_Col_12: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 1;
	in->DataType = CalpontSystemCatalog::CHAR;
	in->OutputType = OT_DATAVALUE;
 	in->BOP = BOP_AND;
	in->NOPS = 2;
	in->NVALS = 0;
	in->InputFlags = 0;

	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);
	args->COP = COMPARE_GT;
/*	args->val[0] = 0xFC;*/
//	We need to test char values if the data type is char
	args->val[0] = '2';		
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 2]);
	args->COP = COMPARE_LT;
// 	args->val[0] = 0;
	args->val[0] = '4';
	
	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = &output[sizeof(NewColResultHeader)];
//  	cout << "NVALS = " << out->NVALS << endl;
 	CPPUNIT_ASSERT(out->NVALS == 32);
	for (i = 0; i < out->NVALS; i++) {
//  		cout << i << ": " << hex << (int)results[i] << dec << endl;
/* 		CPPUNIT_ASSERT(results[i] == 0xFD);
		cout << i << ": " << (int)results[i] << endl;		*/
		CPPUNIT_ASSERT( (int)'3'  == results[i]  );  
	}

	close(fd);
}

void p_Col_13()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	int16_t *results;
	uint32_t written, i;
	int fd;
	int32_t tmp;
	int16_t ridTmp;
	string filename("col4block.cdf");

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_13: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_13: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_13: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_13: could not read a whole block" << endl;
		throw runtime_error("p_Col_13: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);	

	in->DataSize = 4;
	in->DataType = CalpontSystemCatalog::INT;
	in->OutputType = OT_RID;
	in->NOPS = 3;
	in->BOP = BOP_OR;
	in->NVALS = 3;
	in->InputFlags = 1;

	// first argument "is RID 8 < 10?"  Answer is yes
	tmp = 10;	// value to check
	ridTmp = 8;
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, in->DataSize);
	memcpy(&args->val[in->DataSize], &ridTmp, 2);

	// second argument "is RID 5 > 10?"  Answer is no
	args = reinterpret_cast<ColArgs *>(&args->val[in->DataSize+2]);
	args->COP = COMPARE_GT;
	tmp = 10;
	ridTmp = 5; 
	memcpy(args->val, &tmp, in->DataSize);
	memcpy(&args->val[in->DataSize], &ridTmp, 2);

	// third argument "is RID 11 < 1000?"  Answer is yes
	args = reinterpret_cast<ColArgs *>(&args->val[in->DataSize+2]);
	args->COP = COMPARE_LT;
	tmp = 1000;
	ridTmp = 11; 
	memcpy(args->val, &tmp, in->DataSize);
	memcpy(&args->val[in->DataSize], &ridTmp, 2);

	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<int16_t *>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
  	CPPUNIT_ASSERT(out->NVALS == 2);
 	CPPUNIT_ASSERT(results[0] == 8);
 	CPPUNIT_ASSERT(results[1] == 11);
//  	for (i = 0; i < out->NVALS; i++) {
//     		cout << i << ": " << (int)results[i] << endl;
//       	CPPUNIT_ASSERT(results[i] == (i < 10 ? i : i - 10 + 1001));
//  	}

	close(fd);
}

void p_Col_double_1()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	double *results;
	uint32_t written, i;
	int fd;
	string filename("col_double_block.cdf");
	double tmp;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_double_1: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_double_1: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_12: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_double_1: could not read a whole block" << endl;
		throw runtime_error("p_Col_double_1: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::DOUBLE;
	in->OutputType = OT_DATAVALUE;
 	in->BOP = BOP_AND;
	in->NOPS = 2;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10.5;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);
	args->COP = COMPARE_GT;
	memcpy(args->val, &tmp, sizeof(tmp));
	tmp = 15;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 9]);
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, sizeof(tmp));
	
	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<double*>(&output[sizeof(NewColResultHeader)]);
//   	cout << "NVALS = " << out->NVALS << endl;
   	CPPUNIT_ASSERT(out->NVALS == 8);
	for (i = 0; i < out->NVALS; i++) {
//   		cout << i << ": " << results[i] << dec << endl;
  		CPPUNIT_ASSERT(results[i] == 11 + (i * 0.5));
	}

	close(fd);
}

void p_Col_float_1()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	float *resultVal;
	uint32_t written, i;
	int fd;
	string filename("col_float_block.cdf");
	float tmp;
	int16_t *resultRid;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_float_1: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_float_1: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_float_1: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_float_1: could not read a whole block" << endl;
		throw runtime_error("p_Col_float_1: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 4;
	in->DataType = CalpontSystemCatalog::FLOAT;
	in->OutputType = OT_BOTH;
 	in->BOP = BOP_AND;
	in->NOPS = 2;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = 10.5;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);
	args->COP = COMPARE_GT;
	memcpy(args->val, &tmp, sizeof(tmp));
	tmp = 15;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 5]);
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, sizeof(tmp));
	
	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

//     	cout << "NVALS = " << out->NVALS << endl;
   	CPPUNIT_ASSERT(out->NVALS == 8);
	for (i = 0; i < out->NVALS; i++) {
		resultRid = reinterpret_cast<int16_t *>(&output[
			sizeof(NewColResultHeader) + i * (sizeof(Int16) + in->DataSize)]);
		resultVal = reinterpret_cast<float *>(&resultRid[1]);

//     		cout << i << ":   rid:" << (int) *resultRid << " val:" << *resultVal << endl; 

    	CPPUNIT_ASSERT(*resultVal == 11 + (i * 0.5));
	}

	close(fd);
}

void p_Col_neg_float_1()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	float *resultVal;
	int16_t *resultRid;
	uint32_t written, i;
	int fd;
	string filename("col_neg_float.cdf");
	float tmp;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_neg_float_1: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_neg_float_1: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_float_1: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_neg_float_1: could not read a whole block" << endl;
		throw runtime_error("p_Col_neg_float_1: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 4;
	in->DataType = CalpontSystemCatalog::FLOAT;
	in->OutputType = OT_BOTH;
 	in->BOP = BOP_AND;
 	in->NOPS = 2;
// 	in->NOPS = 0;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = -5.0;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);
	args->COP = COMPARE_GT;
	memcpy(args->val, &tmp, sizeof(tmp));
	tmp = 5.0;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 5]);
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, sizeof(tmp));
	
	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

//     cout << "NVALS = " << out->NVALS << endl;
   	CPPUNIT_ASSERT(out->NVALS == 19);
	for (i = 0; i < out->NVALS; i++) {
		resultRid = reinterpret_cast<int16_t *>(&output[
			sizeof(NewColResultHeader) + i * (sizeof(Int16) + in->DataSize)]);
		resultVal = reinterpret_cast<float *>(&resultRid[1]);

//     		cout << i << ":   rid:" << (int) *resultRid << " val:" << *resultVal << endl; 

   		CPPUNIT_ASSERT(*resultVal == -4.5 + (i * 0.5));
	}

	close(fd);
}

void p_Col_neg_double_1()
{
	PrimitiveProcessor pp;
	uint8_t input[BLOCK_SIZE], output[4*BLOCK_SIZE], block[BLOCK_SIZE];
	NewColRequestHeader *in;
	NewColResultHeader *out;
	ColArgs *args;
	double *results;
	uint32_t written, i;
	int fd;
	string filename("col_neg_double.cdf");
	double tmp;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		cerr << "p_Col_neg_double_1: skipping this test; needs the index list file " 
			<< filename << endl;
// 		throw runtime_error("p_Col_1: file not found...");
		return;
	}

	i = read(fd, block, BLOCK_SIZE);
	if (i <= 0) {
		cerr << "p_Col_neg_double_1: Couldn't read the file " << filename << endl;
		throw runtime_error("p_Col_neg_double_1: Couldn't read the file");
	}
	if (i != BLOCK_SIZE) {
		cerr << "p_Col_neg_double_1: could not read a whole block" << endl;
		throw runtime_error("p_Col_neg_double_1: could not read a whole block");
	}

	memset(input, 0, BLOCK_SIZE);
	memset(output, 0, 4*BLOCK_SIZE);

	in = reinterpret_cast<NewColRequestHeader *>(input);
	out = reinterpret_cast<NewColResultHeader *>(output);
	
	in->DataSize = 8;
	in->DataType = CalpontSystemCatalog::DOUBLE;
	in->OutputType = OT_DATAVALUE;
 	in->BOP = BOP_AND;
	in->NOPS = 2;
	in->NVALS = 0;
	in->InputFlags = 0;

	tmp = -5.0;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader)]);
	args->COP = COMPARE_GT;
	memcpy(args->val, &tmp, sizeof(tmp));
	tmp = 5.0;
	args = reinterpret_cast<ColArgs *>(&input[sizeof(NewColRequestHeader) + 9]);
	args->COP = COMPARE_LT;
	memcpy(args->val, &tmp, sizeof(tmp));
	
	pp.setBlockPtr((int*) block);
	pp.p_Col(in, out, 4*BLOCK_SIZE, &written);

	results = reinterpret_cast<double*>(&output[sizeof(NewColResultHeader)]);
//    	cout << "NVALS = " << out->NVALS << endl;
   	CPPUNIT_ASSERT(out->NVALS == 19);
	for (i = 0; i < out->NVALS; i++) {
//    		cout << i << ": " << results[i] << dec << endl;
   		CPPUNIT_ASSERT(results[i] == -4.5 + (i * 0.5));
	}

	close(fd);
}

void p_Dictionary_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);
	
	outValue = reinterpret_cast<DataValue *>(&results[1]);
	outValue->data[args->len] = '\0';		// not reusable in tests with multiple matches.
 	CPPUNIT_ASSERT(results->NVALS == 1);
 	CPPUNIT_ASSERT(outValue->len == 5);
 	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "XHINA", 5) == 0);
}

void p_Dictionary_2()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 2;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_OR;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_EQ;
	args->len = 13;
	strncpy((char *)args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + args->len + 
		sizeof(DictFilterElement)]);
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);
	
	outValue = reinterpret_cast<DataValue *>(&results[1]);
 	CPPUNIT_ASSERT(results->NVALS == 2);
 	CPPUNIT_ASSERT(outValue->len == 5);
 	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "XHINA", 5) == 0);

	outValue = reinterpret_cast<DataValue *>(&output[sizeof(DictOutput) + sizeof(DataValue) + outValue->len]);
	CPPUNIT_ASSERT(outValue->len == 13);
	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "UNITED STATES", 13) == 0);
}

void p_Dictionary_3()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 0;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//   	cout << "NVALS = " << results->NVALS << endl;
  	CPPUNIT_ASSERT(results->NVALS == 50);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
//   		cout << "  " << i << ": len=" << outValue->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_gt_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_GT;
	args->len = 7;
	strncpy((char *)args->data, "GERMANY", 7);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//  	cout << "NVALS = " << results->NVALS << endl;
 	CPPUNIT_ASSERT(results->NVALS == 41);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
//  		cout << "  " << i << ": len=" << outValue->len << " data=" << tmp << endl;
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_token_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	PrimToken *outToken;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 2;
	cmd->OutputType = OT_TOKEN;
	cmd->BOP = BOP_OR;
	cmd->LBID = 56878;  //random magic
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_EQ;
	args->len = 13;
	strncpy((char *)args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + args->len + 
		sizeof(DictFilterElement)]);
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

	CPPUNIT_ASSERT(results->NVALS == 2);
	
	outToken= reinterpret_cast<PrimToken *>(&results[1]);
	CPPUNIT_ASSERT(outToken->len == 5);
	CPPUNIT_ASSERT(outToken->LBID == cmd->LBID);
	CPPUNIT_ASSERT(outToken->offset == 19);
	CPPUNIT_ASSERT(outToken[1].len == 13);
	CPPUNIT_ASSERT(outToken[1].LBID == cmd->LBID);
	CPPUNIT_ASSERT(outToken[1].offset == 50);
}

void p_Dictionary_inputArg_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 2;
	cmd->OutputType = OT_INPUTARG;
	cmd->BOP = BOP_OR;
	cmd->LBID = 56878;  //random magic
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_EQ;
	args->len = 13;
	strncpy((char *)args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + args->len + 
		sizeof(DictFilterElement)]);
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

	CPPUNIT_ASSERT(results->NVALS == 2);
	
	outValue = reinterpret_cast<DataValue *>(&results[1]);
 	CPPUNIT_ASSERT(results->NVALS == 2);
 	CPPUNIT_ASSERT(outValue->len == 5);
 	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "XHINA", 5) == 0);

	outValue = reinterpret_cast<DataValue *>(&output[sizeof(DictOutput) + sizeof(DataValue) + outValue->len]);
	CPPUNIT_ASSERT(outValue->len == 13);
	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "UNITED STATES", 13) == 0);
}

void p_Dictionary_token_agg_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	PrimToken *outToken;
	DictAggregate *agg;
	DataValue *outValue;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 2;
	cmd->OutputType = OT_TOKEN | OT_AGGREGATE;
	cmd->BOP = BOP_OR;
	cmd->LBID = 56878;  //random magic
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_EQ;
	args->len = 13;
	strncpy((char *)args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + args->len + 
		sizeof(DictFilterElement)]);
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

	CPPUNIT_ASSERT(results->NVALS == 2);
	
	outToken = reinterpret_cast<PrimToken *>(&results[1]);
	CPPUNIT_ASSERT(outToken->len == 5);
	CPPUNIT_ASSERT(outToken->LBID == cmd->LBID);
	CPPUNIT_ASSERT(outToken->offset == 19);
	CPPUNIT_ASSERT(outToken[1].len == 13);
	CPPUNIT_ASSERT(outToken[1].LBID == cmd->LBID);
	CPPUNIT_ASSERT(outToken[1].offset == 50);

	agg = reinterpret_cast<DictAggregate *>(&outToken[2]);
  	CPPUNIT_ASSERT(agg->Count == 50);
	
	// min follows
	outValue = reinterpret_cast<DataValue *>(&agg[1]);
  	CPPUNIT_ASSERT(outValue->len == 7);
  	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "ALGERIA", 7) == 0);
	// max follows
	outValue = reinterpret_cast<DataValue *>(((int8_t *)outValue) + 
		sizeof(DataValue) + outValue->len);
  	CPPUNIT_ASSERT(outValue->len == 6);
  	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "XUSSIA", 6) == 0);
}

void p_Dictionary_inToken_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DictAggregate *agg;
	PrimToken *outToken;
	DataValue *outValue;
	int fd;
	uint32_t err;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + 3*sizeof(PrimToken)]);

	cmd->NOPS = 2;
	cmd->OutputType = OT_TOKEN | OT_AGGREGATE;
	cmd->BOP = BOP_OR;
	cmd->LBID = 56878;  //random magic
	cmd->NVALS = 3;
	cmd->InputFlags = 0;
	cmd->tokens[0].LBID = 56878;
	cmd->tokens[0].offset = 7838;	// UNITED STATES
	cmd->tokens[0].len = 13;
	cmd->tokens[1].LBID = 56878;
	cmd->tokens[1].offset = 7890;	// ROMANIA
	cmd->tokens[1].len = 7;
	cmd->tokens[2].LBID = 56878;
	cmd->tokens[2].offset = 8074;	// XHINA
	cmd->tokens[2].len = 5;

	args->COP = COMPARE_EQ;
	args->len = 13;
	strncpy((char *)args->data, "UNITED STATES", 13);

	args = reinterpret_cast<DictFilterElement *>(&input[sizeof(DictInput) + 3*sizeof(PrimToken) + 
		args->len + sizeof(DictFilterElement)]);
	args->COP = COMPARE_EQ;
	args->len = 5;
	strncpy((char *)args->data, "XHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

	CPPUNIT_ASSERT(results->NVALS == 2);
	
	outToken= reinterpret_cast<PrimToken *>(&results[1]);
 	CPPUNIT_ASSERT(outToken[0].len == 13);
 	CPPUNIT_ASSERT(outToken[0].LBID == cmd->LBID);
 	CPPUNIT_ASSERT(outToken[0].offset == 50);
 	CPPUNIT_ASSERT(outToken[1].len == 5);
 	CPPUNIT_ASSERT(outToken[1].LBID == cmd->LBID);
 	CPPUNIT_ASSERT(outToken[1].offset == 19);

	agg = reinterpret_cast<DictAggregate *>(&outToken[2]);
  	CPPUNIT_ASSERT(agg->Count == 3);
	
	// min follows
	outValue = reinterpret_cast<DataValue *>(&agg[1]);
  	CPPUNIT_ASSERT(outValue->len == 7);
  	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "ROMANIA", 7) == 0);
	// max follows
	outValue = reinterpret_cast<DataValue *>(((int8_t *)outValue) + 
		sizeof(DataValue) + outValue->len);
  	CPPUNIT_ASSERT(outValue->len == 5);
  	CPPUNIT_ASSERT(strncmp((char *)outValue->data, "XHINA", 5) == 0);
}

void p_Dictionary_oldgetsig_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;

#pragma pack(push,1)
	struct OldSigArgs {
		uint64_t rid;
		uint16_t offsetIndex;
	} *args;
#pragma pack(pop)
	
	struct OldResults {
		uint64_t rid;
		DataValue value;
	} *oldResults;

	int fd, argsOffset;
	uint32_t err;
// 	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<OldSigArgs *>(&cmd[1]);

	cmd->NOPS = 0;
	cmd->OutputType = OT_RID | OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 2;
	cmd->InputFlags = 1;
	args[0].rid = 0x1234567891011121LL;
	args[0].offsetIndex = 48;   	// RUSSIA
	args[1].rid = 0x1121110987654321LL;
	args[1].offsetIndex = 38;		// JAPAN

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

// 	cout << " nvals = " << results->NVALS << endl;

	CPPUNIT_ASSERT(results->NVALS == 2);
	argsOffset = sizeof(DictOutput);

	oldResults = reinterpret_cast<OldResults *>(&output[argsOffset]);
// 	strncpy(tmp, oldResults->value.data, oldResults->value.len);
// 	tmp[oldResults->value.len] = '\0';
// 	cout << " oldResults.rid = " << oldResults->rid << endl;
// 	cout << " oldResults.value = " << tmp << endl;	
	CPPUNIT_ASSERT(oldResults->value.len == 6);
	CPPUNIT_ASSERT(strncmp(oldResults->value.data, "RUSSIA", 6) == 0);
	CPPUNIT_ASSERT(oldResults->rid == 0x1234567891011121LL);
	argsOffset += 8 + 2 + oldResults->value.len;


	oldResults = reinterpret_cast<OldResults *>(&output[argsOffset]);
// 	strncpy(tmp, oldResults->value.data, oldResults->value.len);
// 	tmp[oldResults->value.len] = '\0';
// 	cout << " oldResults.rid = " << oldResults->rid << endl;
// 	cout << " oldResults.value = " << tmp << endl;
	CPPUNIT_ASSERT(oldResults->value.len == 5);
	CPPUNIT_ASSERT(strncmp(oldResults->value.data, "JAPAN", 5) == 0);
	CPPUNIT_ASSERT(oldResults->rid == 0x1121110987654321LL);
}

void p_Dictionary_like_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 5;
	strncpy((char *)args->data, "%HINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
 	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
//    		cout << "  " << i << ": len=" << outValue->len << " data=" << tmp << endl;
 		if (i == 0)
 			CPPUNIT_ASSERT(strncmp(tmp, "XHINA", 5) == 0);
 		else if (i == 1)
 			CPPUNIT_ASSERT(strncmp(tmp, "CHINA", 5) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_2()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 5;
	strncpy((char *)args->data, "%HIN%", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
 	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
//    		cout << "  " << i << ": len=" << outValue->len << " data=" << tmp << endl;
 		if (i == 0)
 			CPPUNIT_ASSERT(strncmp(tmp, "XHINA", 5) == 0);
 		else if (i == 1)
 			CPPUNIT_ASSERT(strncmp(tmp, "CHINA", 5) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_3()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 4;
	strncpy((char *)args->data, "CHI%", 4);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
  	CPPUNIT_ASSERT(results->NVALS == 1);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
		CPPUNIT_ASSERT(strncmp(tmp, "CHINA", 5) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_4()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 5;
	strncpy((char *)args->data, "CHINA", 5);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
  	CPPUNIT_ASSERT(results->NVALS == 1);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
		CPPUNIT_ASSERT(strncmp(tmp, "CHINA", 5) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_5()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 7;
	strncpy((char *)args->data, "_NDO%A%", 7);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
   	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
// 		cout << "  " << tmp << endl;
		if (i == 0)
	 		CPPUNIT_ASSERT(strncmp(tmp, "XNDONESIA", 9) == 0);
		else
			CPPUNIT_ASSERT(strncmp(tmp, "INDONESIA", 9) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_6()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 11;
	strncpy((char *)args->data, "%NIT%ING%D%", 11);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
   	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
// 		cout << "  " << tmp << endl;
		if (i == 0)
	 		CPPUNIT_ASSERT(strncmp(tmp, "XNITED KINGDOM", 14) == 0);
		else
			CPPUNIT_ASSERT(strncmp(tmp, "UNITED KINGDOM", 14) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_7()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 7;
	strncpy((char *)args->data, "UNI%TES", 7);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
   	CPPUNIT_ASSERT(results->NVALS == 1);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
// 		cout << "  " << tmp << endl;
	 	CPPUNIT_ASSERT(strncmp(tmp, "UNITED STATES", 13) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_8()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	DataValue *outValue;
	int fd, argsOffset;
	uint32_t err, i;
	char tmp[BLOCK_SIZE];

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 7;
	strncpy((char *)args->data, "%TH_OP%", 7);

	pp.setBlockPtr((int *) block);
	pp.p_Dictionary(cmd, results, BLOCK_SIZE);

//    	cout << "NVALS = " << results->NVALS << endl;
   	CPPUNIT_ASSERT(results->NVALS == 2);

	argsOffset = sizeof(DictOutput);
	for (i = 0; i < results->NVALS; i++) {
		outValue = reinterpret_cast<DataValue *>(&output[argsOffset]);
		strncpy((char *) tmp, outValue->data, outValue->len);
		tmp[outValue->len] = '\0';
//  		cout << "  " << tmp << endl;
		if (i == 0)
		 	CPPUNIT_ASSERT(strncmp(tmp, "XTHIOPIA", 8) == 0);
		else 
			CPPUNIT_ASSERT(strncmp(tmp, "ETHIOPIA", 8) == 0);
		argsOffset += sizeof(DataValue) + outValue->len;
	}
}

void p_Dictionary_like_prefixbench_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
	int fd;
	uint32_t err;
	uint64_t count;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 4;
	strncpy((char *)args->data, "CHI%", 4);

	pp.setBlockPtr((int *) block);

	done = 0;
	count = 0;
	signal(SIGALRM, alarm_handler);
	alarm(30);
	while (!done) {
		pp.p_Dictionary(cmd, results, BLOCK_SIZE);
		count++;
	}
	cout << endl << "  - did a LIKE comparison looking for CHI%, " << count << " times in 30 seconds" << endl;
}

void p_Dictionary_like_substrbench_1()
{
	PrimitiveProcessor pp;
	uint8_t block[BLOCK_SIZE], output[BLOCK_SIZE], input[BLOCK_SIZE];
	string filename("dictblock.cdf");
	DictInput *cmd;
	DictOutput *results;
	DictFilterElement *args;
 	int fd;
	uint32_t err;
	uint64_t count;

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		return;
	}
	err = read(fd, block, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		return;
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
 		return;
	}
	close(fd);

	cmd = reinterpret_cast<DictInput *>(input);
	results = reinterpret_cast<DictOutput *>(output);
	args = reinterpret_cast<DictFilterElement *>(&cmd[1]);

	cmd->NOPS = 1;
	cmd->OutputType = OT_DATAVALUE;
	cmd->BOP = BOP_NONE;
	cmd->NVALS = 0;
	cmd->InputFlags = 0;
	args->COP = COMPARE_LIKE;
	args->len = 4;
	strncpy((char *)args->data, "%HI%", 4);

	pp.setBlockPtr((int *) block);

	done = 0;
	count = 0;
	signal(SIGALRM, alarm_handler);
	alarm(30);
	while (!done) {
		pp.p_Dictionary(cmd, results, BLOCK_SIZE);
		count++;
	}
	cout << endl << "  - did a LIKE comparison looking for %HI%, " << count << " times in 30 seconds" << endl;
}


};

CPPUNIT_TEST_SUITE_REGISTRATION( PrimTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


