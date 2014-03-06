/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***************************************************************************
 *
 *   $Id: fileblockrequestqueue.cpp 666 2008-07-22 14:39:46Z wweeks $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/


#include "fileblockrequestqueue.h"

using namespace std;

namespace dbbc {


fileBlockRequestQueue::fileBlockRequestQueue() : queueSize(0), readersWaiting(0)
{
	pthread_mutex_init(&mutex, NULL);
	pthread_cond_init(&notEmpty, NULL);
}

fileBlockRequestQueue::~fileBlockRequestQueue()
{
	pthread_cond_destroy(&notEmpty);
	pthread_mutex_destroy(&mutex);
}

bool fileBlockRequestQueue::empty() const
{
	return (queueSize == 0);
}

fileRequest* fileBlockRequestQueue::top() const
{
	return fbQueue.front();
}

int fileBlockRequestQueue::push(fileRequest& blk) {
	pthread_mutex_lock(&mutex);
	fbQueue.push_back(&blk);

        // @bug 1007.  Changed "== 1" to ">= 1" below.  The wake up call was only being fired when the queue size was 1 which
        // caused only one i/o thread to be working at a time. 
	if (++queueSize >= 1 && readersWaiting > 0) 
		pthread_cond_signal(&notEmpty);
	pthread_mutex_unlock(&mutex);
	
	return 0;
}

void fileBlockRequestQueue::stop() {
	pthread_cond_broadcast(&notEmpty);
}


fileRequest* fileBlockRequestQueue::pop(void) {
	pthread_mutex_lock(&mutex);
	while (queueSize == 0) {
		readersWaiting++;
		pthread_cond_wait(&notEmpty, &mutex);
		readersWaiting--;
	}

 	fileRequest* blk = fbQueue.front();
 	fbQueue.pop_front();
	--queueSize;
	pthread_mutex_unlock(&mutex);
	return blk;
}

}
