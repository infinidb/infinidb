/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: constantfilter.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#include <exception>
#include <stdexcept>
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include "constantfilter.h"
#include "returnedcolumn.h"
#include "operator.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "bytestream.h"
#include "objectreader.h"
#include "aggregatecolumn.h"

namespace {
template<class T> struct deleter : public unary_function<T&, void>
{
	void operator()(T& x) { delete x; }
};
}

namespace execplan {

/**
 * Constructors/Destructors
 */
ConstantFilter::ConstantFilter()
{}

ConstantFilter::ConstantFilter(const SOP& op, ReturnedColumn* lhs, ReturnedColumn* rhs)
{
    SSFP ssfp(new SimpleFilter(op, lhs, rhs));
    fFilterList.push_back(ssfp);
    SimpleColumn* sc = dynamic_cast<SimpleColumn*>(lhs);
    fCol.reset(sc->clone());
}

ConstantFilter::ConstantFilter(SimpleFilter* sf)
{
    SSFP ssfp(sf);
    fFilterList.push_back(ssfp);
    const SimpleColumn* sc = dynamic_cast<const SimpleColumn*>(sf->lhs());
    fCol.reset(sc->clone());
}

ConstantFilter::ConstantFilter(const ConstantFilter& rhs): 
	fOp(rhs.fOp),
	fCol(rhs.fCol)
{
	fFilterList.clear();
	fSimpleColumnList.clear();
	SSFP ssfp;
	for (uint i = 0; i < rhs.fFilterList.size(); i++)
	{
		ssfp.reset(rhs.fFilterList[i]->clone());
		fFilterList.push_back(ssfp);
		fSimpleColumnList.insert(fSimpleColumnList.end(), 
		                         ssfp->simpleColumnList().begin(), 
		                         ssfp->simpleColumnList().end());
	}
}

ConstantFilter::~ConstantFilter()
{
}

/**
 * Methods
 */
 
const string ConstantFilter::toString() const
{
	ostringstream output;
	output << "ConstantFilter" << endl;
	if (fOp) output << "  " << *fOp << endl;
	if (!fFunctionName.empty()) output << "  Func: " << fFunctionName << endl;
	if (fCol) output << "   " << *fCol << endl;
    for (unsigned int i = 0; i<fFilterList.size(); i++)
	    output << "  " << *fFilterList[i] << endl;

	return output.str();
}

//const string ConstantFilter::data() const
//{
   //string out = "";
   // if (fOp) out = fOp->data();
   // if (fFilterList.size() != 0)
	 //   return string(out + " " + fFilterList.front()->lhs()->data());
   // return "ConstantFilter";	    
   //return fData;
//}

ostream& operator<<(ostream& output, const ConstantFilter& rhs)
{
    /*
    output << rhs.op()->toString() << endl;
    output << rhs.col()->toString() << endl;
    for (unsigned int i = 0; i < rhs.filterList().size(); i++)
        output << rhs.filterList()[i]->toString() << endl;
	return output;*/
	output << rhs.toString();
	return output;
}

void ConstantFilter::serialize(messageqcpp::ByteStream& b) const
{
	FilterList::const_iterator it;
	b << static_cast<ObjectReader::id_t>(ObjectReader::CONSTANTFILTER);
	Filter::serialize(b);
	if (fOp != NULL)
		fOp->serialize(b);
	else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);
	if (fCol != NULL)
	    fCol->serialize(b);
    else
		b << static_cast<ObjectReader::id_t>(ObjectReader::NULL_CLASS);	    
	b << static_cast<u_int32_t>(fFilterList.size());
	for (it = fFilterList.begin(); it != fFilterList.end(); it++)
		(*it)->serialize(b);
	b << fFunctionName;
}
	
void ConstantFilter::unserialize(messageqcpp::ByteStream& b)
{
	u_int32_t size, i;
	ObjectReader::checkType(b, ObjectReader::CONSTANTFILTER);
	SimpleFilter *sc;
	
	Filter::unserialize(b);
	fOp.reset(dynamic_cast<Operator*>(ObjectReader::createTreeNode(b)));
	fCol.reset(dynamic_cast<SimpleColumn*>(ObjectReader::createTreeNode(b)));
	b >> size;
	fFilterList.clear();
	fSimpleColumnList.clear();
	fAggColumnList.clear();
	for (i = 0; i < size; i++) {
		sc = dynamic_cast<SimpleFilter*>(ObjectReader::createTreeNode(b));
		SSFP ssfp(sc);
		fFilterList.push_back(ssfp);
		fSimpleColumnList.insert(fSimpleColumnList.end(), 
		                         ssfp->simpleColumnList().begin(), 
		                         ssfp->simpleColumnList().end());
		fAggColumnList.insert(fAggColumnList.end(), 
		                         ssfp->aggColumnList().begin(), 
		                         ssfp->aggColumnList().end());
	}
	b >> fFunctionName;
}

bool ConstantFilter::operator==(const ConstantFilter& t) const
{
	const Filter *f1, *f2;
	FilterList::const_iterator it, it2;

	f1 = static_cast<const Filter*>(this);
	f2 = static_cast<const Filter*>(&t);
	if (*f1 != *f2)
		return false;
	
	if (fOp != NULL) {
		if (*fOp != *t.fOp)
			return false;
	}
	else if (t.fOp != NULL)
		return false;
	
	//fFilterList
	if (fFilterList.size() != t.fFilterList.size())
		return false;
	for	(it = fFilterList.begin(), it2 = t.fFilterList.begin();
		it != fFilterList.end(); ++it, ++it2)
    {
        if (**it != **it2)
            return false;
    }
	
	return true;
}

bool ConstantFilter::operator==(const TreeNode* t) const
{
	const ConstantFilter *o;

	o = dynamic_cast<const ConstantFilter*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool ConstantFilter::operator!=(const ConstantFilter& t) const
{
	return (!(*this == t));
}

bool ConstantFilter::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

Filter* ConstantFilter::combinable(Filter* f, Operator *op)
{
    if (typeid(*f) == typeid(SimpleFilter))
    {
        SimpleFilter *sf = dynamic_cast<SimpleFilter*>(f);
        return sf->combinable(this, op);
    }
    
    // constant filter may have NULL fOp, when it's equalivant to a pure simple filter    
    if  (typeid(ConstantFilter) == typeid(*f) && 
        (!fOp || fOp->data().compare(op->data()) == 0 ))
    {
        ConstantFilter *cf = dynamic_cast<ConstantFilter*>(f);
        if (cf && ( !cf->op() || !fOp || fOp->data().compare(cf->op()->data()) == 0 )
            && fCol->sameColumn(cf->col().get()))
        {
            for (unsigned int i = 0; i < cf->filterList().size(); i++)
                fFilterList.push_back(cf->filterList()[i]);
            fOp.reset(op);
            return this;
        }
    }

    return NULL;       
}

} // namespace execplan
// vim:ts=4 sw=4:
