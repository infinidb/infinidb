#include <iostream>
#include <stdexcept>
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

int main(int argc, char** argv)
{
	Config* cf = Config::makeConfig("./Calpont.xml");
	MessageQueueClient mqc("server1", cf);

	ByteStream obs;
	string msg("Hello, world!");
	ByteStream ibs;
	uint32_t qb;
	for (int i = 0; i < 10; i++)
	{
		obs.restart();
		obs << msg;
		cout << "writing " << obs.length() << " bytes to " << mqc.addr2String() << endl;
		mqc.write(obs);
		ibs = mqc.read();
		ibs >> qb;
		if (qb != 0)
		{
			string emsg("server did not ack message!");
			cerr << emsg << endl;
			throw runtime_error(emsg);
		}
	}

	return 0;
}

