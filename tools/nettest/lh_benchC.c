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

long long int count = 0, xferred = 0;
int timer = 0;

void ding(int sig)
{
	timer++;
	printf("transferred %lld msgs in %d seconds (%d/s) bytes = %lld\n", 
		count, timer, count/timer, xferred);
	alarm(1);
}

void sigpipe(int sig)
{
	printf("exiting...\n");
	exit(0);
}

int main() {
	int fd, err;
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
	err = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
	if (err < 0) {
		perror ("connect");
		exit(1);
	}
	
	signal(SIGALRM, ding);
	signal(SIGPIPE, sigpipe);
	alarm(1);
	while (1) {
		err = write(fd, buf, MSGSIZE);
		if (err < 0) {
			perror("write");
			exit(1);
		}
		xferred += MSGSIZE;
		count++;
		err = read(fd, buf, MSGSIZE);
		if (err < 0) {
			perror("read");
			exit(1);
		}
		xferred += MSGSIZE;
		count++;
	}
	return 0;
}









