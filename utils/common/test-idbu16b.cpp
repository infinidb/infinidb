#include <iostream>
//#define NDEBUG
#include <cassert>
#include <limits>
#include <string>
#include <arpa/inet.h>
#include <cstring>
#include <tr1/unordered_set>
using namespace std;

#include <boost/functional/hash.hpp>
using namespace boost;

#include "idbu16b.h"
using namespace utils;

int main(int argc, char** argv)
{
	idbu16b f1,f2,f3;
	idbu16b zero;
	idbu16b one(0, 1);
	idbu16b two(0, 2);

	assert(f1 == zero);

	f2.lo++;
	assert(f2 > zero);
	assert(f2 > f1);
	assert(zero < f2);
	assert(zero <= f2);
	assert(f2 != zero);

	f3.hi++;
	assert(f3 > zero);
	assert(f3 > f2);
	assert(f3 != f2);
	assert(f2 <= f3);

	idbu16b f4(1,1);
	f3 += f2;
	assert(f3 == f4);

	idbu16b f5(0, numeric_limits<uint64_t>::max());
	idbu16b f6(1, 0);

	assert(f5 < f6);
	f5 += f2;
	assert(f5 == f6);

	f5.hi = 0;
	f5.lo = numeric_limits<uint64_t>::max();
	f5++;
	assert(f5 == f6);
	f5.hi = 0;
	f5.lo = numeric_limits<uint64_t>::max();
	++f5;
	assert(f5 == f6);

	f1.lo = 1;
	f1.hi = 0;
	f1--;
	assert(f1 == zero);

	f1.lo = 0;
	f1.hi = 1;
	f1 -= one;
	assert(f1.hi == 0 && f1.lo == numeric_limits<uint64_t>::max());

	f1.lo = 0;
	f1.hi = 0;
	assert(f1 == zero);
	assert(++f1 == one);
	assert(f1++ == one);
	assert(f1 > one);

	assert(one + one == two);
	assert(two - one == one);
	assert(two - two == zero);
	assert(two - one - one == zero);

	unsigned char buf[16];
	idbu16b* p1=0;
	p1 = reinterpret_cast<idbu16b*>(&buf[0]);
	*p1 = zero;
	assert(*p1 == zero);
	++*p1;
	assert(*p1 == one);
	assert(buf[0] == '\001');

	f1.lo = 0;
	f1.hi = 1;
	*p1 = f1;
	assert(*p1 > zero);
	assert(*p1 > one);
	assert(buf[8] == '\001');
	f1.hi = 100;
	assert(f1 > *p1);

	string uuidstrin("4460f793-1830-4648-964f-70da44d4db9a");
	uuidstrin =      "00000000-0001-0001-0000-000000000001";
	string uuidstrout;
	idbu16b s1(idbu16b::fromUUIDString(uuidstrin));
	//cout << "s:  " << uuidstrin << endl;
	//cout << "s1: " << s1.toString() << endl;
	//cout << "s1: " << s1.toUUIDString() << endl;
	assert(uuidstrin == s1.toUUIDString());

	uuidstrin =      "4460f793-1830-4648-964f-70da44d4db9a";
	s1 = idbu16b::fromUUIDString(uuidstrin);
	//cout << "s:  " << uuidstrin << endl;
	//cout << "s1: " << s1.toString() << endl;
	//cout << "s1: " << s1.toUUIDString() << endl;
	assert(uuidstrin == s1.toUUIDString());

	f1.hi = 0x4460f79318304648ull;
	f1.lo = 0x964f70da44d4db9aull;

	assert (f1 == s1);

	string ipv6str = "fe80::1d1b:7051:e5d4:b8af";
	s1 = idbu16b::fromIPv6String(ipv6str);
	//cout << "s:  " << ipv6str << endl;
	//cout << "s1: " << s1.toString() << endl;
	//cout << "s1: " << s1.toIPv6String() << endl;
	assert(ipv6str == s1.toIPv6String());

	int rc;
	struct in6_addr addr;
	memset(&addr, 0, sizeof(addr));
	rc = inet_pton(AF_INET6, ipv6str.c_str(), &addr);
	assert(rc == 1);
	assert(memcmp(&addr, &s1.lo, 16) == 0);

	f1 = s1;

	hash<idbu16b> idbu16b_hasher;
	assert(f1 == s1);
	assert(idbu16b_hasher(f1) == idbu16b_hasher(s1));

	// this should also work via ADL: tr1::unordered_set<idbu16b> uset;
	tr1::unordered_set<idbu16b, hash<idbu16b> > uset;
	uset.insert(f1);
	assert(uset.size() == 1);
	uset.insert(f2);
	assert(uset.size() == 2);
	uset.insert(s1); //same as f1
	assert(uset.size() == 2);

	f1.hi = 1024;
	f1.lo = 66;
	f2.hi = 1024;
	f2.lo = 66;
	assert(f1 == f2);
	assert(f2 == f1);
	assert(f1 <= f2);
	assert(!(f1 < f2));
	assert(f1 >= f2);
	assert(!(f1 > f2));

	f2.lo++;
	assert(f1 != f2);
	assert(f2 != f1);
	assert(f1 <= f2);
	assert(f1 < f2);
	assert(!(f1 >= f2));
	assert(!(f1 > f2));
	assert(f2 >= f1);
	assert(f2 > f1);

	return 0;
}

