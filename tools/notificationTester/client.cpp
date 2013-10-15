#include <iostream>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "liboamcpp.h"
using namespace oam;

int main(int argc, char** argv)
{
	Oam oam;
	int rc;

	rc = oam.sendDeviceNotification("PM1", START_PM_MASTER_DOWN, "This is a test");

	assert(rc == API_SUCCESS);

	return 0;
}

// vim:ts=4 sw=4:
