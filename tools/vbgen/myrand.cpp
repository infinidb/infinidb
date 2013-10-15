#include "myrand.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
using namespace std;

namespace myrand
{

MyRand::MyRand(int min, int max) :
	fMin(min),
	fMax(max)
{
	if (fMax < fMin) throw range_error("max<min");
	int fd;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) throw runtime_error("open");
	read(fd, &fSeed, 4);
	close(fd);
}

}
// vim:ts=4 sw=4:

