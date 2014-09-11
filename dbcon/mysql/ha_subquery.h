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

/***********************************************************************
*   $Id: ha_subquery.h 6418 2010-03-29 21:55:08Z zzhu $
*
*
***********************************************************************/
/** @file */
/** class subquery series interface */

#ifndef HA_SUBQUERY
#define HA_SUBQUERY

//#undef LOG_INFO
#include "idb_mysql.h"
#include "ha_calpont_impl_if.h"

namespace execplan
{
	class PredicateOperator;
}

namespace cal_impl_if
{

/** @file */

class SubQuery
{
public:
	SubQuery() : fGwip(NULL), fCorrelated(false) {}
	virtual ~SubQuery() { delete fGwip; }
	virtual gp_walk_info* gwip() const { return fGwip; }
	virtual void gwip(gp_walk_info* gwip) { fGwip = gwip; }
	const bool correlated() const { return fCorrelated; }
	void correlated (const bool correlated) { fCorrelated = correlated; }
	virtual void handleFunc (gp_walk_info* gwip, Item_func* func) {}
	virtual void handleNot () {}
protected:
	gp_walk_info* fGwip;
	bool fCorrelated;
};

/**
 * @brief A class to represent a generic WHERE clause subquery
 */
class WhereSubQuery : public SubQuery
{
public:	
	WhereSubQuery() : SubQuery(),
		                fSub(NULL), 
		                fFunc(NULL) {}
	WhereSubQuery(const execplan::SRCP& column, Item_subselect* sub, Item_func* func) : 
	                  SubQuery(),
	                  fColumn(column), 
	                  fSub(sub), 
	                  fFunc(func) {}
	WhereSubQuery(Item_func* func) : fFunc(func) {}
	WhereSubQuery(Item_subselect* sub) : fSub(sub) {} // for exists
	virtual ~WhereSubQuery() {}
	
	/** Accessors and mutators */
	virtual Item_subselect* sub() const { return fSub; }
	virtual void sub(Item_subselect* sub) { fSub = sub; }
	virtual Item_func* func() const { return fFunc; }
	virtual void func(Item_func* func) { fFunc = func; }
	virtual execplan::ParseTree* transform() = 0;
protected:
	execplan::SRCP fColumn;
	Item_subselect* fSub;
	Item_func* fFunc;
};

/**
 * @brief A class to represent a scalar subquery
 */
class ScalarSub : public WhereSubQuery
{
public:
	ScalarSub();
	ScalarSub(Item_func* func);
	ScalarSub(const execplan::SRCP& column, Item_subselect* sub, Item_func* func);
	ScalarSub(const ScalarSub& rhs);
	~ScalarSub();
	execplan::ParseTree* transform();
	execplan::ParseTree* transform_between();
	execplan::ParseTree* transform_in();
	execplan::ParseTree* buildParseTree(execplan::PredicateOperator* op);
	const uint64_t returnedColPos() const { return fReturnedColPos; }
	void returnedColPos(const uint64_t returnedColPos) {fReturnedColPos = returnedColPos;}
	
private:
	uint64_t fReturnedColPos;
};

/**
 * @brief A class to represent a IN subquery
 */
class InSub : public WhereSubQuery
{
public:	
	InSub();
	InSub(Item_func* func);
	InSub(const InSub& rhs);
	~InSub();
	execplan::ParseTree* transform();
	void handleFunc(gp_walk_info* gwip, Item_func* func);
	void handleNot();
};

/**
 * @brief A class to represent an EXISTS subquery
 */
class ExistsSub : public WhereSubQuery
{
public:
	ExistsSub(); // not complete. just for compile
	ExistsSub(Item_subselect* sub);
	~ExistsSub();
	execplan::ParseTree* transform();
	void handleNot();
};

/**
 * @brief A class to represent a subquery which contains GROUP BY
 */
class AggregateSub : public WhereSubQuery
{
};

/**
 * @brief A class to represent a generic FROM subquery
 */
class FromSubQuery : public SubQuery
{
public:	
	FromSubQuery();
	FromSubQuery(SELECT_LEX* fromSub);
	~FromSubQuery();
	const SELECT_LEX* fromSub() const { return fFromSub; }
	void fromSub(SELECT_LEX* fromSub) {fFromSub = fromSub; }
	const std::string alias () const { return fAlias; }
	void alias (const std::string alias) { fAlias = alias; }
	execplan::CalpontSelectExecutionPlan* transform();
private:
	SELECT_LEX* fFromSub;
	std::string fAlias;
};

class SelectSubQuery :  public SubQuery
{
public:
	SelectSubQuery();
	SelectSubQuery(Item_subselect* sel);
	~SelectSubQuery();
	execplan::CalpontSelectExecutionPlan* transform();
	Item_subselect* selSub() { return fSelSub; }
	void selSub( Item_subselect* selSub ) { fSelSub = selSub; }
private:
	Item_subselect* fSelSub;
};

}

#endif
