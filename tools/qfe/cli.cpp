#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <icmpapi.h>
#include <stdio.h>
#else
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif
#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>
using namespace std;

#include "socktype.h"
#include "socketio.h"
using namespace qfe;

namespace
{
void usage()
{
	cout << "usage: cli [-h] [-s serverip] [-p port] [-c schema] <query>" << endl;
	cout << '\t' << "-s serverip connect to serverip instead of 127.0.0.1" << endl;
	cout << '\t' << "-p port     connect on port instead of 9198" << endl;
	cout << '\t' << "-c schema   use schema as default instead of tpch1" << endl;
	cout << '\t' << "-h          display this help" << endl;
}
}

int main(int argc, char** argv)
{
	opterr = 0;
	int c;
	string serverip("127.0.0.1");
	string schema("tpch1");
	short PortNo = 9198;
	while ((c = getopt(argc, argv, "s:c:p:h")) != -1)
		switch (c)
		{
		case 's':
			serverip = optarg;
			break;
		case 'c':
			schema = optarg;
			break;
		case 'p':
			PortNo = atoi(optarg);
			if (PortNo == 0)
			{
				usage();
				return 1;
			}
			break;
		case 'h':
		case '?':
		default:
			usage();
			return 0;
			break;
		}

	if (argc - optind < 1)
	{
		usage();
		return 1;
	}

	string query(argv[optind+0]);

	SockType fd = -1;

#ifdef _MSC_VER
	WSAData wsadata;
	const WORD minVersion = MAKEWORD(2, 2);
	if (WSAStartup(minVersion, &wsadata) != 0)
		cerr << "networking startup error: " << IDBSysErrorStr(WSAGetLastError()) << endl;
#endif
	fd = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0)
	{
#ifdef _MSC_VER
		cerr << "socket create error: " << IDBSysErrorStr(WSAGetLastError()) << endl;
#else
		cerr << "socket create error: " << strerror(errno) << endl;
#endif
		return 1;
	}

	int rc = 0;
	struct sockaddr_in serv_addr;
	struct addrinfo hints;
	struct addrinfo* res=0;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = PF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	rc = getaddrinfo(serverip.c_str(), 0, &hints, &res);
	if (rc != 0)
	{
		cerr << "Error resolving '" << serverip << "': " << gai_strerror(rc) << endl;
		return 1;
	}
	sockaddr_in* sain=0;
	sain = reinterpret_cast<sockaddr_in*>(res->ai_addr);
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = PF_INET;
	serv_addr.sin_addr = sain->sin_addr;
	serv_addr.sin_port = htons(PortNo);
	freeaddrinfo(res);

	rc = ::connect(fd, (sockaddr*)&serv_addr, sizeof(serv_addr));
	if (rc < 0)
	{
#ifdef _MSC_VER
		cerr << "socket connect error: " << IDBSysErrorStr(WSAGetLastError()) << endl;
		WSACleanup();
#else
		cerr << "socket connect error: " << strerror(errno) << endl;
#endif
		return 1;
	}

	socketio::writeString(fd, schema);
	socketio::writeString(fd, query);

	uint32_t flag=0;
	string row;
	row = socketio::readString(fd);
	if (row != "OK")
	{
		cerr << "query failed: " << row << endl;
		goto bailout;
	}

	row = socketio::readString(fd);
	while (!row.empty())
	{
		cout << row << endl;
		row = socketio::readString(fd);
	}

	flag=0;
	SockWriteFcn(fd, &flag, 4);
bailout:
	::shutdown(fd, SHUT_RDWR);
#ifdef _MSC_VER
	::closesocket(fd);
	WSACleanup();
#else
	::close(fd);
#endif
	fd = -1;

	return 0;
}

