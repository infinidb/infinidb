#include <iostream>
//#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <cstring>
#include <string>
using namespace std;

#include "idb_getopt.h"

/*
       int getopt1(int argc, char * const argv[],
                  const char *optstring) __THROW;

       extern char *optarg;
       extern int optind, opterr, optopt;
*/


int main(int argc, char** argv)
{
	opterr = 0;
	int c;
	bool invalid = false;
	bool aflg = false;
	bool bflg = false;
	bool cflg = false;
	bool dflg = false;
	string dval;
	bool eflg = false;
	string eval;
	char invchar = 0;
#if defined(TEST1)
	char* opts = "";
	char* av[] = { 0 };
#elif defined(TEST2)
	char* opts = "-a+bcd:e:";
//	char* av[] = { "prg", "-ab", "-dfoo", "-xe", "bar", "file1", "file2", 0 };
//	char* av[] = {"prg", "file1", "file2"};
#else
#error Need a test to run!
#endif
//	int ac = sizeof(av) / sizeof(av[0]) - 1;
	
	while ((c = getopt(argc, argv, opts)) != -1)
		switch (c)
		{
		case 'a':
			aflg = true;
			break;
		case 'b':
			bflg = true;
			break;
		case 'c':
			cflg = true;
			break;
		case 'd':
			dflg = true;
			dval = optarg;
			break;
		case 'e':
			eflg = true;
			eval = optarg;
			break;
		case '?':
			invalid = true;
			invchar = optopt;
			break;
		default:
			invalid = true;
			invchar = 0;
			break;
		}

#if defined(TEST1)
	assert(!invalid && !aflg && !bflg && !cflg);
	assert(optind == 1);
#elif defined(TEST2)
/*	cout << "optind=" << optind << endl;
	assert(invalid && aflg && bflg && !cflg);
	assert((ac - optind) == 2);
	assert(string(av[optind+0]) == "file1");
	assert(string(av[optind+1]) == "file2");
	assert(dflg);
	assert(dval == "foo");
	assert(eflg);
	assert(eval == "bar");
	assert(invchar == 'x');*/
	cout << "aflag=" << aflg << " bflag=" << bflg << " cflg=" << cflg
             << " dflag=" << dflg << " eflag=" << eflg << endl;
        cout << " eval=" << eval << " dval=" << dval << endl;
        cout << " invchar=" << invchar << endl;
        cout << " final optind=" << optind << endl;
#endif

	return 0;
}


