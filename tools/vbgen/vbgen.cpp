#include <unistd.h>
#include <iostream>
#include <iomanip>
#include <limits>
#include <vector>
#include <algorithm>
using namespace std;

#include "myrand.h"
using namespace myrand;

namespace
{
class PrintIt
{
public:
	PrintIt(ostream& os) : fOs(os)
	{
		fOs.fill('0');
		fOs.flags(ios::hex);
	}
	~PrintIt() { }

	void operator()(const int i) const
	{
		unsigned u = static_cast<unsigned>(i & 0xff);
		fOs << setw(2) << u;
	}

private:
	//Defaults okay
	//PrintIt(const PrintIt& rhs);
	//PrintIt& operator=(const PrintIt& rhs);

	ostream& fOs;
};
}

int main(int argc, char** argv)
{
	int c;
	unsigned long long numRows = 20000000;

	opterr = 0;

	while ((c = getopt(argc, argv, "r:h")) != -1)
		switch (c)
		{
		case 'r':
			numRows = strtoull(optarg, 0, 0);
			break;
		case 'h':
		case '?':
		default:
			break;
		}

	MyRand mrwidth(100, 800);

	for (unsigned long long i = 0; i < numRows; i++)
	{
		int w = mrwidth.generate();
		vector<int> vi(w);
		generate(vi.begin(), vi.end(), MyRand(0, 255));
		cout << "0|";
		for_each(vi.begin(), vi.end(), PrintIt(cout));
		cout << '|' << endl;
	}

	return 0;
}

