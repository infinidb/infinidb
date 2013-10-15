#include <iostream>
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

int main(int argc, char** argv)
{
	Config* cf = Config::makeConfig("./Calpont.xml");
	MessageQueueServer mqs("server1", cf);

	cout << "server ready..." << endl;

	IOSocket ios;
	ByteStream ibs;
	ByteStream obs;
	uint32_t qb = 0;
	while (1)
	{
		ios = mqs.accept();
		ibs = ios.read();
		while (ibs.length() > 0)
		{
			cout << "read " << ibs.length() << " bytes from " << ios << endl;
			obs.restart();
			obs << qb;
			ios.write(obs);
			ibs = ios.read();
		}
		ios.close();
	}

	return 0;
}

