#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cerrno>
//#define NDEBUG
#include <cassert>
#include <cstring>

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <inttypes.h>
#include <cstdlib>
using namespace std;

#include <boost/scoped_array.hpp>

namespace
{
size_t exp_buf_size;
unsigned vflg;

const string lzo_rctos(int r)
{
	switch (r)
	{
	case LZO_E_INPUT_NOT_CONSUMED:
		return "LZO_E_INPUT_NOT_CONSUMED";
	default:
		break;
	}
	return "Unknown Error!";
}

void usage()
{
	cout << "usage: idb_comp [-e size][-h] file(s)" << endl
		<< "\t-e size size (in KB) of expansion buffer" << endl
		<< "\t-h      display this help" << endl;
}

int doit(const string& infile)
{
	int rc = 0;
	int ifd = open(infile.c_str(), O_RDONLY|O_LARGEFILE|O_DIRECT);

	if (ifd < 0)
	{
		cerr << infile << ": open: " << strerror(errno) << endl;
		return 1;
	}

	struct stat istatbuf;
	fstat(ifd, &istatbuf);

	string outname(infile);

	string::size_type ptr;

	ptr = outname.find_last_of('.');

	if (ptr != string::npos)
		outname.erase(ptr);

	ptr = outname.find_last_of('/');

	if (ptr != string::npos)
		outname.erase(0, ptr+1);

	outname = "./" + outname + ".cmp";

	int ofd = open(outname.c_str(), O_WRONLY|O_CREAT|O_TRUNC|O_LARGEFILE|O_DIRECT, 0644);

	if (ofd < 0)
	{
		cerr << outname << ": open: " << strerror(errno) << endl;
		close(ifd);
		return 1;
	}

	lzo_init();

	ssize_t nread = -1;
	ssize_t nwritten = -1;
	lzo_bytep ibuf;
	lzo_bytep tibuf;
	lzo_bytep cbuf;
	lzo_bytep tcbuf;
	lzo_bytep wkmem;
	lzo_uint32 ibuf_len = 0;
	lzo_uint cbuf_len = 0;

	ibuf_len = 512 * 1024 * 8;
	tibuf = new lzo_byte[ibuf_len + 4095];
	if ((ptrdiff_t)tibuf & 0xfffULL)
		ibuf = (lzo_bytep)((ptrdiff_t)tibuf & 0xfffffffffffff000ULL) + 4096;
	else
		ibuf = tibuf;
	cbuf_len = 512 * 1024 * 8;
	tcbuf = new lzo_byte[cbuf_len + 4095 + exp_buf_size * 1024];
	if ((ptrdiff_t)tcbuf & 0xfff)
		cbuf = (lzo_bytep)((ptrdiff_t)tcbuf & 0xfffffffffffff000ULL) + 4096;
	else
		cbuf = tcbuf;
	wkmem = new lzo_byte[LZO1X_1_15_MEM_COMPRESS];

	int r = LZO_E_OK;

	const int TOTAL_HDR_LEN = 4096 * 2;
	char* hdrbuf = new char[TOTAL_HDR_LEN + 4095];
	memset(hdrbuf, 0, TOTAL_HDR_LEN + 4095);
	char* hdrbufp = 0;
	if ((ptrdiff_t)hdrbuf & 0xfff)
		hdrbufp = (char*)((ptrdiff_t)hdrbuf & 0xfffffffffffff000ULL) + 4096;
	else
		hdrbufp = hdrbuf;

	struct compHdr
	{
		uint64_t ptrs[512];
	};

	idbassert(sizeof(compHdr) <= 4096);

	compHdr* hdrptr1 = (compHdr*)hdrbufp;
	compHdr* hdrptr  = hdrptr1 + 1; // advance to 2nd hdr to store compression ptrs
	lseek(ofd, TOTAL_HDR_LEN, SEEK_SET);

	nread = read(ifd, ibuf, ibuf_len);

	int idx = 0;
	off_t cmpoff = TOTAL_HDR_LEN;
	while (nread > 0)
	{
		cbuf_len = 512 * 1024 * 8;
		memset(cbuf, 0, cbuf_len);
		r = lzo1x_1_15_compress(ibuf, nread, cbuf, &cbuf_len, wkmem);
		if (r != LZO_E_OK)
		{
			cerr << "compression failed!: " << r << endl;
			rc = 1;
			goto out;
		}
		if (cbuf_len > (unsigned)nread)
		{
			cerr << "WARNING: expansion detected! (output grew by " << (cbuf_len - nread) << " bytes)" << endl;
			idbassert((cbuf_len - nread) <= exp_buf_size * 1024);
		}
		if (cbuf_len & 0xfff)
			cbuf_len = (cbuf_len & 0xfffffffffffff000ULL) + 4096;
		//cbuf_len = 512 * 1024 * 8;
		nwritten = write(ofd, cbuf, cbuf_len);
		if (nwritten < 0 || (unsigned)nwritten != cbuf_len)
		{
			cerr << outname << ": write: " << strerror(errno) << " (" << nwritten << ')' << endl;
			rc = 1;
			goto out;
		}
		if (vflg > 0)
		{
			lzo_bytep tbuf;
			lzo_uint tbuflen = 4 * 1024 * 1024 + 4;
			boost::scoped_array<lzo_byte> tbuf_sa(new lzo_byte[tbuflen]);
			tbuf = tbuf_sa.get();
			cout << "idx: " << idx << " off: " << cmpoff << " size: " << cbuf_len;
			r = lzo1x_decompress(cbuf, cbuf_len, tbuf, &tbuflen, 0);
			cout << " r: " << lzo_rctos(r) << " size: " << tbuflen << endl;
		}
		hdrptr->ptrs[idx] = cmpoff;
		idx++;
		cmpoff += cbuf_len;

		nread = read(ifd, ibuf, ibuf_len);
	}

	if (nread < 0)
	{
		cerr << infile << ": read: " << strerror(errno) << endl;
		rc = 1;
		goto out;
	}

	hdrptr->ptrs[idx] = cmpoff;
	idbassert(idx <= 64);

	// Fill in meta-data information in first header
	hdrptr1->ptrs[0] = 0xfdc119a384d0778eULL;
	hdrptr1->ptrs[1] = 1;
	hdrptr1->ptrs[2] = 1;

	nwritten = pwrite(ofd, hdrbufp, TOTAL_HDR_LEN, 0);
	idbassert(nwritten == TOTAL_HDR_LEN);

out:
	delete [] wkmem;
	delete [] tcbuf;
	delete [] tibuf;
	fsync(ofd);
	struct stat ostatbuf;
	fstat(ofd, &ostatbuf);
	idbassert(ostatbuf.st_size == (signed)hdrptr->ptrs[idx]);
	delete [] hdrbuf;
	cout << infile << ": Input Size: " << istatbuf.st_size
		<< " Output size: " << ostatbuf.st_size
		<< " Compression: " << (100LL - (ostatbuf.st_size * 100LL / istatbuf.st_size)) << '%' << endl;
	close(ofd);
	close(ifd);

	return rc;
}

}

int main(int argc, char** argv)
{
	opterr = 0;
	int c;
	exp_buf_size = 128;
	vflg = 0;

	while ((c = getopt(argc, argv, "e:vh")) != -1)
		switch (c)
		{
		case 'e':
			exp_buf_size = atoi(optarg);
			break;
		case 'v':
			vflg++;
			break;
		case 'h':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) < 1)
	{
		usage();
		return 1;
	}

	int rc = 0;

	for (int i = optind; i < argc; i++)
		if (doit(argv[i]))
			rc = 1;

	return rc;
}

