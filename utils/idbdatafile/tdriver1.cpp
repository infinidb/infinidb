#include <iostream>
#include <iomanip>
#include <dlfcn.h>
using namespace std;

#include "IDBPolicy.h"

int main(int argc, char** argv)
{
	void* dl=0;
#if 0
	dl = dlopen("./libjvm.so", RTLD_NOW);
	if (!dl)
	{
		cerr << "dlopen(): " << dlerror() << endl;
		return 1;
	}
#endif
#if 0
	dl = dlopen("./libhdfs.so", RTLD_LAZY|RTLD_GLOBAL);
	if (!dl)
	{
		cerr << "dlopen(): " << dlerror() << endl;
		return 1;
	}
#endif
	bool ret = false;
	ret = idbdatafile::IDBPolicy::installPlugin("hdfs-20.so");

	cout << "ret = " << boolalpha << ret << endl;

	return 0;
}

