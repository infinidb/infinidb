/*

   Copyright (C) 2009-2013 Calpont Corporation.

   Use of and access to the Calpont InfiniDB Community software is subject to the
   terms and conditions of the Calpont Open Source License Agreement. Use of and
   access to the Calpont InfiniDB Enterprise software is subject to the terms and
   conditions of the Calpont End User License Agreement.

   This program is distributed in the hope that it will be useful, and unless
   otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
   the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   Please refer to the Calpont Open Source License Agreement and the Calpont End
   User License Agreement for more details.

   You should have received a copy of either the Calpont Open Source License
   Agreement or the Calpont End User License Agreement along with this program; if
   not, it is your responsibility to review the terms and conditions of the proper
   Calpont license agreement by visiting http://www.calpont.com for the Calpont
   InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
   the Calpont InfiniDB Community Calpont Open Source License Agreement.

   Calpont may make changes to these license agreements from time to time. When
   these changes are made, Calpont will make a new copy of the Calpont End User
   License Agreement available at http://www.calpont.com and a new copy of the
   Calpont Open Source License Agreement available at http:///www.infinidb.org.
   You understand and agree that if you use the Program after the date on which
   the license agreement authorizing your use has changed, Calpont will treat your
   use as acceptance of the updated License.

*/

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
