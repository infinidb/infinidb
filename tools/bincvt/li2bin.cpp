#include <iostream>
#ifdef _MSC_VER
#include <boost/cstdint.hpp>
#include <fcntl.h>
#include <io.h>
#else
#include <inttypes.h>
#endif
#include <string>
#include <cstring>
#include <cstdlib>
//#define NDEBUG
#include <cassert>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/static_assert.hpp>
using namespace boost;

#ifdef _MSC_VER
namespace
{
	inline long long atoll(const char* s)
	{
		return _strtoi64(s, 0, 10);
	}
	inline uint64_t strtoull(const char* s, char** a, int b)
	{
		return _strtoi64(s, a, b);
	}
}
#endif

//1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|

#pragma pack(1)
struct lineitem_image
{
	int32_t l_orderkey;
	int32_t l_partkey;
	int32_t l_suppkey;             //12
	int64_t l_linenumber;
	int64_t l_quantity;
	int64_t l_extendedprice;
	int64_t l_discount;
	int64_t l_tax;                //40
	char l_returnflag;
	char l_linestatus;            //2
	int32_t l_shipdate;
	int32_t l_commitdate;
	int32_t l_receiptdate;        //12
	char l_shipinstruct[25];
	char l_shipmode[10];
	char l_comment[44];           //79
};
BOOST_STATIC_ASSERT(sizeof(struct lineitem_image) == 145);

// version of lineitem_image that treats some integer columns as unsigned ints.
// Decimal, date, and date/time columns are still treated as integer.
struct unsigned_lineitem_image
{
	uint32_t l_orderkey;
	uint32_t l_partkey;
	uint32_t l_suppkey;           //12
	uint64_t l_linenumber;
	int64_t  l_quantity;
	int64_t  l_extendedprice;
	int64_t  l_discount;
	int64_t  l_tax;               //40
	char     l_returnflag;
	char     l_linestatus;        //2
	int32_t  l_shipdate;
	int32_t  l_commitdate;
	int32_t  l_receiptdate;       //12
	char     l_shipinstruct[25];
	char     l_shipmode[10];
	char     l_comment[44];       //79
};
BOOST_STATIC_ASSERT(sizeof(struct unsigned_lineitem_image) == 145);

struct Date
{
    unsigned spare  : 6;
    unsigned day    : 6;
    unsigned month  : 4;
    unsigned year   : 16;
};
BOOST_STATIC_ASSERT(sizeof(struct Date) == 4);

union date_image
{
	struct Date d;
	int32_t i;
};

namespace
{
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

int64_t idbbigdec2(const string& str)
{
	int64_t val=0;
	string::size_type idx=string::npos;
	string tmp(str);
	idx = tmp.find('.');
	if (idx != string::npos)
		tmp.erase(idx, 1);
	else
		tmp.append("00");
	val = atoll(tmp.c_str());
	return val;
}

int32_t idbdate(const string& str)
{
	date_image di;
	di.i = 0;
	boost::char_separator<char> sep("-");
	tokenizer tokens(str, sep);
	tokenizer::iterator tok_iter = tokens.begin();

	// Note that the spare bits must be set, instead of leaving them
	// initialized to 0; to be compatible with internal date format.
	if (distance(tok_iter, tokens.end()) == 3)
	{
		di.d.spare = 0x3E;
		di.d.year = atoi(tok_iter->c_str()); ++tok_iter;
		di.d.month = atoi(tok_iter->c_str()); ++tok_iter;
		di.d.day = atoi(tok_iter->c_str()); ++tok_iter;
	}
	
	return di.i;
}

int parseinto(lineitem_image& img, const string& buf)
{
	memset(&img, 0, sizeof(img));

	boost::char_separator<char> sep("|");
	tokenizer tokens(buf, sep);
	tokenizer::iterator tok_iter = tokens.begin();

	if (distance(tok_iter, tokens.end()) < 16)
		return -1;

	img.l_orderkey = atoi(tok_iter->c_str()); ++tok_iter;
	img.l_partkey = atoi(tok_iter->c_str()); ++tok_iter;
	img.l_suppkey = atoi(tok_iter->c_str()); ++tok_iter;
	img.l_linenumber = atoll(tok_iter->c_str()); ++tok_iter;
	img.l_quantity = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_extendedprice = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_discount = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_tax = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_returnflag = tok_iter->at(0); ++tok_iter;
	img.l_linestatus = tok_iter->at(0); ++tok_iter;
	img.l_shipdate = idbdate(tok_iter->c_str()); ++tok_iter;
	img.l_commitdate = idbdate(tok_iter->c_str()); ++tok_iter;
	img.l_receiptdate = idbdate(tok_iter->c_str()); ++tok_iter;
	memcpy(&img.l_shipinstruct[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;
	memcpy(&img.l_shipmode[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;
	memcpy(&img.l_comment[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;

	return 0;
}

int unsigned_parseinto(unsigned_lineitem_image& img, const string& buf)
{
	memset(&img, 0, sizeof(img));

	boost::char_separator<char> sep("|");
	tokenizer tokens(buf, sep);
	tokenizer::iterator tok_iter = tokens.begin();

	if (distance(tok_iter, tokens.end()) < 16)
		return -1;

	img.l_orderkey      = strtoul(tok_iter->c_str(),0,10); ++tok_iter;
	img.l_partkey       = strtoul(tok_iter->c_str(),0,10); ++tok_iter;
	img.l_suppkey       = strtoul(tok_iter->c_str(),0,10); ++tok_iter;
	img.l_linenumber    = strtoull(tok_iter->c_str(),0,10); ++tok_iter;
	img.l_quantity      = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_extendedprice = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_discount      = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_tax           = idbbigdec2(tok_iter->c_str()); ++tok_iter;
	img.l_returnflag    = tok_iter->at(0); ++tok_iter;
	img.l_linestatus    = tok_iter->at(0); ++tok_iter;
	img.l_shipdate      = idbdate(tok_iter->c_str()); ++tok_iter;
	img.l_commitdate    = idbdate(tok_iter->c_str()); ++tok_iter;
	img.l_receiptdate   = idbdate(tok_iter->c_str()); ++tok_iter;
	memcpy(&img.l_shipinstruct[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;
	memcpy(&img.l_shipmode[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;
	memcpy(&img.l_comment[0], tok_iter->c_str(), tok_iter->size()); ++tok_iter;

	return 0;
}

}

int main(int argc, char** argv)
{
	if ((argc > 1) && (strcmp(argv[1],"-h") == 0))
	{
		std::cerr << "li2bin [-u]" << std::endl;
		std::cerr << "  -u Create first 4 fields as unsigned integers" << std::endl;
		return 0;
	}

	string input;

#ifdef _MSC_VER
	_setmode(1, _O_BINARY);
#endif

	getline(cin, input);
	if ((argc > 1) && (strcmp(argv[1],"-u") == 0))
	{
		unsigned_lineitem_image i;
		while (!cin.eof())
		{
			if (unsigned_parseinto(i, input) == 0)
				cout.write(reinterpret_cast<const char*>(&i), sizeof(i));
			getline(cin, input);
		}
	}
	else
	{
		lineitem_image i;
		while (!cin.eof())
		{
			if (parseinto(i, input) == 0)
				cout.write(reinterpret_cast<const char*>(&i), sizeof(i));
			getline(cin, input);
		}
	}

	return 0;
}
// vim:ts=4 sw=4:

