/* Copyright (C) 2013 Calpont Corp.

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
 * Observer.h
 *
 *  Created on: Oct 5, 2011
 *      Author: bpaul@calpont.com
 */

#ifndef OBSERVER_H_
#define OBSERVER_H_

#include <list>
using namespace std;


namespace WriteEngine
{

class Subject; 	// forward deceleration

class Observer {
public:
	virtual ~Observer();
	virtual bool update(Subject* pSub)=0;

protected:
	Observer();
};

class Subject {
public:
	Subject();
	virtual ~Subject();

	virtual void attach(Observer* Obs);
	virtual void detach(Observer* Obs);
	virtual void notify();


private:
	typedef std::list<Observer*> Observers;
	Observers fObs;
};

}

#endif /* OBSERVER_H_ */
