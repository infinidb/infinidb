#include <iostream>
#include <stdexcept>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
//#define NDEBUG
#include <cassert>
#include <cerrno>
using namespace std;

/*
	Protocol definition:
	On the control fifo:
	This server waits for the other to send it:
		1. The name of the data fifo to open and read/write (string)
		2. The number of bytes of compressed data to read (number)

	On the data fifo:
	The server then reads the compressed data from the data fifo:
		1. The compressed data
	then it decompresses it, and sends back to the client:
		1. The number of bytes in the uncompressed stream (number)
		2. The uncompressed data

	strings are sent like this:
		uint32_t string len
		<?> len bytes of the string
	numbers are sent like this:
		uint64_t the number

	This server expects numeric values to be in its native byte order, so
	   the sender needs to do it that way.
*/

namespace
{
const string MessageFifo("/tmp/idbdsfifo");
}

int main(int argc, char** argv)
{
again:
	int fd = open(MessageFifo.c_str(), O_WRONLY|O_NONBLOCK);
	if (fd < 0)
	{
		if (errno == ENXIO)
		{
			cerr << "waiting for DS to startup..." << endl;
			sleep(1);
			goto again;
		}
		throw runtime_error("while opening fifo for write");
	}
	uint32_t u32;
	uint64_t u64;
	string s;
	ssize_t wrc;

	s = "/tmp/cdatafifo";
	mknod(s.c_str(), S_IFIFO|0666, 0);
	u32 = s.length();
	wrc = write(fd, &u32, 4);
	assert(wrc == 4);
	wrc = write(fd, s.c_str(), u32);
	assert(wrc == u32);

	u64 = 707070;
	write(fd, &u64, 8);

	close(fd);

	fd = open(s.c_str(), O_WRONLY);
	assert(fd >= 0);

	char* b = new char[u64];
	assert(b);

	wrc = write (fd, b, u64);
	assert(wrc == u64);

	delete [] b;

	close(fd);
	fd = open(s.c_str(), O_RDONLY);
	assert(fd >= 0);

	wrc = read(fd, &u64, 8);
	assert(wrc == 8);

	b = new char[u64];
	assert(b);

	cout << "going to read " << u64 << " bytes of uncompressed data" << endl << flush;
	wrc = read(fd, b, u64);
	assert(wrc == u64);
	cout << "read " << u64 << " bytes of uncompressed data" << endl;

	delete [] b;

	close(fd);

	unlink(s.c_str());

	return 0;
}

