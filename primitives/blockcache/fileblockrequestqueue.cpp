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
 *   $Id: fileblockrequestqueue.cpp 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "fileblockrequestqueue.h"

using namespace std;

namespace dbbc {


fileBlockRequestQueue::fileBlockRequestQueue() : queueSize(0), readersWaiting(0)
{
	//pthread_mutex_init(&mutex, NULL);
	//pthread_cond_init(&notEmpty, NULL);
}

fileBlockRequestQueue::~fileBlockRequestQueue()
{
	//pthread_cond_destroy(&notEmpty);
	//pthread_mutex_destroy(&mutex);
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
	mutex.lock(); //pthread_mutex_lock(&mutex);
	fbQueue.push_back(&blk);

        // @bug 1007.  Changed "== 1" to ">= 1" below.  The wake up call was only being fired when the queue size was 1 which
        // caused only one i/o thread to be working at a time. 
	if (++queueSize >= 1 && readersWaiting > 0) 
		notEmpty.notify_one(); //pthread_cond_signal(&notEmpty);
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
	
	return 0;
}

void fileBlockRequestQueue::stop() {
	notEmpty.notify_all(); //pthread_cond_broadcast(&notEmpty);
}


fileRequest* fileBlockRequestQueue::pop(void) {
	mutex.lock(); //pthread_mutex_lock(&mutex);
	while (queueSize == 0) {
		readersWaiting++;
		notEmpty.wait(mutex); //pthread_cond_wait(&notEmpty, &mutex);
		readersWaiting--;
	}

 	fileRequest* blk = fbQueue.front();
 	fbQueue.pop_front();
	--queueSize;
	mutex.unlock(); //pthread_mutex_unlock(&mutex);
	return blk;
}

}
