/*
 * FEMsgHandler.h
 *
 *  Created on: Mar 2, 2011
 *      Author: pleblanc
 */

#ifndef FEMSGHANDLER_H_
#define FEMSGHANDLER_H_

#include "joblist.h"
#include "inetstreamsocket.h"

class FEMsgHandler
{
public:
	FEMsgHandler();
	FEMsgHandler(boost::shared_ptr<joblist::JobList>, messageqcpp::IOSocket *);
	virtual ~FEMsgHandler();

	void start();
	void stop();
	void setJobList(boost::shared_ptr<joblist::JobList>);
	void setSocket(messageqcpp::IOSocket *);
	bool aborted();

	void threadFcn();

private:
	bool die, running, sawData;
	messageqcpp::IOSocket *sock;
	boost::shared_ptr<joblist::JobList> jl;
	boost::thread thr;
	boost::mutex mutex;
};

#endif /* FEMSGHANDLER_H_ */
