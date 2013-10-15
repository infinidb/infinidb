/*****************************************************************************
* $Id: main.cpp 1739 2012-03-22 12:57:59Z pleblanc $
*
*****************************************************************************/
#include <iostream>
#include <unistd.h>
//#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <fnmatch.h>
#include <cassert>
#include <boost/tokenizer.hpp>
#include <stack>
#include <stdexcept>
using namespace std;
using namespace boost;

#include "configcpp.h"
using namespace config;

#include "extentmap.h"
using namespace BRM;

#include "calpontsystemcatalog.h"
#include "dm.h"

namespace
{

unsigned vflg = 0;
bool dflg = false;
string DBRoot;
string pattern;
unsigned extentSize;
ExtentMap em;

void usage(const string& pname)
{
	cout << "usage: " << pname << " [-vdh]" << endl <<
		"   rebuilds the extent map from the contents of the database file system." << endl <<
		"   -v display verbose progress information. More v's, more debug" << endl <<
		"   -d display what would be done--don't do it" << endl <<
		"   -h display this help text" << endl;
}

OID_t pname2OID(const string& fpath)
{
	dmFilePathArgs_t args;
	char aBuff[10];
	char bBuff[10];
	char cBuff[10];
	char fnBuff[16];
	int rc;
	OID_t oid;

	args.pDirA = aBuff;
	args.pDirB = bBuff;
	args.pDirC = cBuff;
	args.pFName = fnBuff;
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);
	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	typedef tokenizer<char_separator<char> > tokenizer;
	char_separator<char> sep("/");
	tokenizer tokens(fpath, sep);
	tokenizer::iterator tok_iter = tokens.begin();
	tokenizer::iterator end = tokens.end();
	typedef stack<string> pcomps_t;
	pcomps_t pcomps;
	while (tok_iter != end)
	{
		pcomps.push(*tok_iter);
		++tok_iter;
	}

	idbassert(pcomps.size() >= 4);

	string pcomp;
	pcomp = pcomps.top();
	pcomps.pop();
	idbassert(pcomp.size() < 16);
	strcpy(args.pFName, pcomp.c_str());
	pcomp = pcomps.top();
	pcomps.pop();
	idbassert(pcomp.size() < 10);
	strcpy(args.pDirC, pcomp.c_str());
	pcomp = pcomps.top();
	pcomps.pop();
	idbassert(pcomp.size() < 10);
	strcpy(args.pDirB, pcomp.c_str());
	pcomp = pcomps.top();
	pcomps.pop();
	idbassert(pcomp.size() < 10);
	strcpy(args.pDirA, pcomp.c_str());

	rc = dmFPath2Oid(&args, (UINT32*)&oid);
	if (rc != 0)
	{
		oid = 0;
	}

	return oid;
}

bool isCalpontDBFile(const string& fpath)
{
	return (fnmatch(pattern.c_str(), fpath.c_str(), 0) == 0);
}

int walkDB(const char* fp, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
{
	string fpath(fp);
	unsigned numExtents;
	unsigned numBlocks;

	if (typeflag != FTW_F)
	{
		return FTW_CONTINUE;
	}

	if (!isCalpontDBFile(fpath))
	{
		if (vflg > 2)
		{
			cout << "Skipping non-Calpont DB file " << fpath << endl;
		}
		return FTW_CONTINUE;
	}

	if (vflg)
	{
		cout << "Processing file " << fpath << endl;
		numBlocks = (sb->st_size + (BLOCK_SIZE - 1)) / BLOCK_SIZE;
		numExtents = (numBlocks + (extentSize - 1)) / extentSize;
		if (vflg > 1)
		{
			cout << "File is " << numExtents << " extent" << (numExtents > 1 ? "s" : "") <<
				", " << numBlocks << " block" << (numBlocks > 1 ? "s" : "") <<
				", " << sb->st_size << " bytes, ";
		}
	}

	OID_t oid;
	oid = pname2OID(fpath);

	if (vflg > 1)
	{
		cout << "OID is " << oid << ", ";
	}

	if (oid <= 10)
	{
		if (vflg)
		{
			cout << endl << "OID " << oid << " is probably a VBBF and is being skipped" << endl;
		}
		return FTW_CONTINUE;
	}

	HWM_t hwm;
#ifdef DELETE_FIRST
	try
	{
		hwm = em.getHWM(oid);
	}
	catch (exception& ex)
	{
		if (vflg)
		{
			cout << endl << "There was no HWM for OID " << oid <<
				", it is probably a VBBF and is being skipped: " << ex.what() << endl;
		}
		return FTW_CONTINUE;
	}
	catch (...)
	{
		if (vflg)
		{
			cout << endl << "There was no HWM for OID " << oid <<
				", it is probably a VBBF and is being skipped" << endl;
		}
		return FTW_CONTINUE;
	}
#else
	hwm = numBlocks - 1;
#endif

	if (vflg > 1)
	{
		cout << "HWM is " << hwm << endl;
	}

#ifdef DELETE_FIRST
	if (vflg > 1)
	{
		cout << "Deleting OID " << oid << " from Extent Map" << endl;
	}

	if (!dflg)
	{
		em.deleteOID(oid);
		em.confirmChanges();
	}
#endif

	vector<LBID_t> lbids;
	int allocdsize = 0;

	if (vflg > 1)
	{
		cout << "Creating extent for " << numBlocks << " blocks for OID " << oid << endl;
	}

	if (!dflg)
	{
		em.createExtent(numBlocks, oid, lbids, allocdsize);
		em.confirmChanges();
	}

	if (vflg > 1)
	{
		cout << "Created " << allocdsize << " LBIDs for OID " << oid << endl;
		if (vflg > 2)
		{
			vector<LBID_t>::iterator iter = lbids.begin();
			vector<LBID_t>::iterator end = lbids.end();
			cout << "First LBIDs from each extent are ";
			while (iter != end)
			{
				cout << *iter << ", ";
				++iter;
			}
			cout << endl;
		}
	}

	if (vflg > 1)
	{
		cout << "Setting OID " << oid << " HWM to " << hwm << endl;
	}

	if (!dflg)
	{
		em.setHWM(oid, hwm);
		em.confirmChanges();
	}

	return FTW_CONTINUE;
}

}

int main(int argc, char** argv)
{
	int c;
	string pname(argv[0]);

	opterr = 0;

	while ((c = getopt(argc, argv, "vdh")) != EOF)
		switch (c)
		{
		case 'v':
			vflg++;
			break;
		case 'd':
			dflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage(pname);
			return (c == 'h' ? 0 : 1);
			break;
		}

	const Config* cf = Config::makeConfig();
	DBRoot = cf->getConfig("SystemConfig", "DBRoot");
	pattern = DBRoot + "/[0-9][0-9][0-9].dir/[0-9][0-9][0-9].dir/[0-9][0-9][0-9].dir/FILE[0-9][0-9][0-9].cdf";

	if (vflg)
	{
		cout << "Using DBRoot " << DBRoot << endl;
	}

	if (access(DBRoot.c_str(), X_OK) != 0)
	{
		cerr << "Could not scan DBRoot " << DBRoot << '!' << endl;
		return 1;
	}

	ExtentMap em;
	extentSize = em.getExtentSize();

	if (vflg)
	{
		cout << "System extent size is " << extentSize << " blocks" << endl;
	}

	if (nftw(DBRoot.c_str(), walkDB, 64, FTW_PHYS|FTW_ACTIONRETVAL) != 0)
	{
		cerr << "Error processing files in DBRoot " << DBRoot << '!' << endl;
		return 1;
	}

	return 0;
}

