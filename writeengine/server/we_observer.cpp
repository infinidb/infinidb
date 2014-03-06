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

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * Observer.cpp
 *
 *  Created on: Oct 5, 2011
 *      Author: bpaul@calpont.com
 */

#include <boost/thread/mutex.hpp>

#include "we_observer.h"


namespace WriteEngine {

//-----------------------------------------------------------------------------

//	ctor
Observer::Observer() {

}

//-----------------------------------------------------------------------------
//dtor
Observer::~Observer() {
	//
}

//-----------------------------------------------------------------------------
//ctor
Subject::Subject(){

}
//-----------------------------------------------------------------------------
//dtor
Subject::~Subject(){

}

//-----------------------------------------------------------------------------

void Subject::attach(Observer* Obs) {
	boost::mutex::scoped_lock aLstLock;
	fObs.push_back(Obs);
}

//-----------------------------------------------------------------------------

void Subject::detach(Observer* Obs) {
	boost::mutex::scoped_lock aLstLock;
	Observers::iterator aIt = fObs.begin();
	while (aIt != fObs.end()) {
		if ((*aIt) == Obs) {
			fObs.erase(aIt);
			break;
		}
	}
}

//-----------------------------------------------------------------------------

void Subject::notify() {
	boost::mutex::scoped_lock aLstLock;
	Observers::iterator aIt = fObs.begin();
	while (aIt != fObs.end()) {
		(*aIt)->update(this);
	}
}

//-----------------------------------------------------------------------------

}// namespace WriteEngine

