#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <string.h>

#define MSGSIZE 10

void sigpipe(int num) 
{
	printf("exiting...\n");
	exit(0);
}

int main() {
	int fd, sock, err;
	socklen_t sl;
	struct sockaddr_in sa;
	char buf[MSGSIZE];

    memset(&sa, 0, sizeof(sa));

    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = htons(62222);
    sa.sin_family = PF_INET;

	fd = socket(PF_INET, SOCK_STREAM, 6);
	if (fd < 0) {
		perror("socket");
		exit(1);
	}
	err = bind(fd, (struct sockaddr *)&sa, (socklen_t) sizeof(sa));
	if (err < 0) {
		perror("bind");
		exit(1);
	}
	sl = sizeof(sa);
	err = listen(fd, 10);
	if (err < 0) {
                perror("listen");
                exit(1);
        }
	sock = accept(fd, (struct sockaddr *)&sa, &sl);
	if (sock < 0) {
		perror("accept");
		exit(1);
	}
	
	signal(SIGPIPE, sigpipe);

	while (1) {
		err = read(sock, buf, MSGSIZE);
		if (err < 0) {
	        perror("read");
   	    	exit(1);
   		}
		err = write(sock, buf, MSGSIZE);
		if (err < 0) {
            perror("write");
            exit(1);
        }
	}
	return 0;
}


	
