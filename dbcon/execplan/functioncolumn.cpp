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
*   $Id: functioncolumn.cpp 9679 2013-07-11 22:32:03Z zzhu $
*
*
***********************************************************************/
#include <string>
#include <iostream>
#include <sstream>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost;

#include "bytestream.h"
#include "functioncolumn.h"
#include "constantcolumn.h"
#include "arithmeticcolumn.h"
#include "simplecolumn.h"
#include "objectreader.h"
#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "windowfunctioncolumn.h"

#include "funcexp.h"
#include "functor_export.h"
using namespace funcexp;

#ifdef _MSC_VER
#define strcasecmp stricmp
#endif

namespace execplan {
/**
 * Constructors/Destructors
 */
FunctionColumn::FunctionColumn()
{}

FunctionColumn::FunctionColumn(string& funcName):
	fFunctionName(funcName)
{}

FunctionColumn::FunctionColumn(const string& functionName, const string& funcParmsInString, const uint32_t sessionID):
	ReturnedColumn(sessionID),
	fFunctionName(functionName),
	fData (functionName + "(" + funcParmsInString + ")"),
	fFunctor(0)
{
	funcParms (funcParmsInString);
}

FunctionColumn::FunctionColumn( const FunctionColumn& rhs, const uint32_t sessionID):
	ReturnedColumn(rhs, sessionID),
	fFunctionName(rhs.functionName()),
	fTableAlias (rhs.tableAlias()),
	fData (rhs.data()),
	fFunctor(rhs.fFunctor)
{
	fFunctionParms.clear();
	fSimpleColumnList.clear();
	fAggColumnList.clear();
	fWindowFunctionColumnList.clear();

	SPTP pt;

	for (uint32_t i = 0; i < rhs.fFunctionParms.size(); i++)
	{
		pt.reset(new ParseTree (*(rhs.fFunctionParms[i])));
		fFunctionParms.push_back(pt);
		pt->walk(getSimpleCols, &fSimpleColumnList);
		pt->walk(getAggCols, &fAggColumnList);
		pt->walk(getWindowFunctionCols, &fWindowFunctionColumnList);
	}
	fAlias = rhs.alias();
}

FunctionColumn::~FunctionColumn()
{}

/**
 * Methods
 */
ostream& operator<<(ostream& output, const FunctionColumn& rhs)
{
	output << rhs.toString();

	return output;
}

const string FunctionColumn::toString() const
{
	ostringstream output;
	output << "FunctionColumn: " << fFunctionName << endl;
	if (fAlias.length() > 0) output << "/Alias: " << fAlias;
	output << "expressionId=" << fExpressionId << endl;
	output << "joinInfo=" << fJoinInfo << " returnAll=" << fReturnAll << " sequence#=" << fSequence << endl;
	output << "resultType=" << colDataTypeToString(fResultType.colDataType) << "|" << fResultType.colWidth << endl;
	output << "operationType=" << colDataTypeToString(fOperationType.colDataType) << endl;
	output << "function parm: " << endl;
	for (uint32_t i = 0; i < fFunctionParms.size(); i++)
		output << fFunctionParms[i]->data()->toString() << endl;
	return output.str();
}

const string FunctionColumn::data() const
{
	return fData;
}

void FunctionColumn::funcParms (const string& funcParmsInString)
{
	// special process to interval function. make the passed in string one literal constant
	if (fFunctionName.compare("interval") == 0)
	{
		SPTP sptp(new ParseTree(new ConstantColumn(funcParmsInString, ConstantColumn::LITERAL)));
		fFunctionParms.push_back(sptp);
		return;
	}

	// try to replace the comma and space in '' to avoid string conflict
	// @bug #396 for space delimited function arguments.
	string funcParam = funcParmsInString;
	string::size_type pos1 = 0;
	string::size_type pos2 = 0;
	string::size_type pos3 = 0;
	while (pos2 < funcParam.length() && pos1 != string::npos)
	{
		pos1 = funcParam.find_first_of("'", pos2);
		pos2 = funcParam.find_first_of("'", pos1+1);
		pos3 = funcParam.find_first_of(", ", pos1);
		if (pos3 < pos2)
		{
			if (funcParam[pos3] == ',')
				funcParam.replace(pos3, 1, "^");
			else if (funcParam[pos3] == ' ')
				funcParam[pos3] = 0x1f; // special char to replace space in quotes
		}
		pos2++;
	}

	// also replace the comma and space in () to avoid function conflict
	unsigned int par = 0;
	for (unsigned int i = 0; i < funcParam.length(); i++)
	{
		if (funcParam[i] == '(')
			par++;
		if (funcParam[i] == ')')
			par--;
		if (funcParam[i] == ',' && par != 0)
			funcParam.replace(i, 1, "^");
		if (funcParam[i] == ' ' && par != 0)
			funcParam[i] = 0x1f;
	}

	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(", ");
	tokenizer tokens(funcParam, sep);

	for (tokenizer::iterator tok_iter = tokens.begin();
		 tok_iter != tokens.end(); ++tok_iter)
	{
		string tok = (*tok_iter);
		// remove the to_upper line. don't know why have it in the first place. for bug #320
		//to_upper(tok);

		// take off the trailing and ending spaces
		std::string::size_type first = tok.find_first_not_of(" ");
		tok = tok.substr(first, tok.find_last_not_of(" ")-first+1);

		// recover '^' to ',' and recover 0x1f to ' '
		for (unsigned int i = 0; i < tok.length(); i++)
		{
			if (tok[i] == '^') tok[i] = ',';
			if (tok[i] == 0x1f) tok[i] = ' ';
		}

		// parse argument
		if (tok[0] == '\'' && tok[tok.length()-1] == '\'')
		{
			// take off quotes
			first = tok.find_first_not_of("'");
			tok = tok.substr(first, tok.find_last_not_of("'")-first+1);
			SPTP cc(new ParseTree(new ConstantColumn(tok, ConstantColumn::LITERAL)));
			fFunctionParms.push_back(cc);
		}
		else if (tok.find("+", 0) != string::npos ||
			tok.find("-", 0) != string::npos ||
			tok.find("*", 0) != string::npos ||
			tok.find("/", 0) != string::npos ||
			tok.find("^", 0) != string::npos ||
			tok.find("(", 0) != string::npos ||
			tok.find(")", 0) != string::npos)
		{
			SPTP ac(new ParseTree(new ArithmeticColumn(tok)));
			fFunctionParms.push_back(ac);
		}
		else if (isdigit(tok[0]) || tok[0] == '.')
		{
			SPTP cc(new ParseTree(new ConstantColumn(tok, ConstantColumn::NUM)));
			fFunctionParms.push_back(cc);
		}
		else if (strcasecmp(tok.c_str(), "NULL") == 0)
		{
			SPTP cc(new ParseTree(new ConstantColumn(tok, ConstantColumn::NULLDATA)));
			fFunctionParms.push_back(cc);
		}
		// map keyword to constant column
		else if (strcasecmp(tok.c_str(), "FROM") == 0 ||
				 strcasecmp(tok.c_str(), "TO") == 0)
		{
			SPTP cc(new ParseTree(new ConstantColumn(tok, ConstantColumn::LITERAL)));
			fFunctionParms.push_back(cc);
		}
		// simplecolumn
		else
		{
			SPTP sc(new ParseTree(new SimpleColumn(tok, fSessionID)));
			fFunctionParms.push_back(sc);
		}
	}
}

void FunctionColumn::serialize(messageqcpp::ByteStream& b) const
{
	b << (ObjectReader::id_t) ObjectReader::FUNCTIONCOLUMN;
	ReturnedColumn::serialize(b);
	b << fFunctionName;

	b << static_cast<uint32_t>(fFunctionParms.size());
	for (uint32_t i = 0; i < fFunctionParms.size(); i++)
		ObjectReader::writeParseTree(fFunctionParms[i].get(), b);
	b << fTableAlias;
	b << fData;
}

void FunctionColumn::unserialize(messageqcpp::ByteStream& b)
{
	uint32_t size, i;
	//SRCP rc;
	SPTP pt;
	FunctionParm::iterator it;

	fFunctionParms.erase(fFunctionParms.begin(), fFunctionParms.end());
	fSimpleColumnList.clear();
	fAggColumnList.clear();
	fWindowFunctionColumnList.clear();

	ObjectReader::checkType(b, ObjectReader::FUNCTIONCOLUMN);
	ReturnedColumn::unserialize(b);
	b >> fFunctionName;

	b >> size;
	for (i = 0; i < size; i++)
	{
		pt.reset(ObjectReader::createParseTree(b));
		fFunctionParms.push_back(pt);
		pt->walk(getSimpleCols, &fSimpleColumnList);
		pt->walk(getAggCols, &fAggColumnList);
		pt->walk(getWindowFunctionCols, &fWindowFunctionColumnList);
	}

	b >> fTableAlias;
	b >> fData;
	FuncExp* funcExp = FuncExp::instance();
	fFunctor = funcExp->getFunctor(fFunctionName);

	// @bug 3506. Special treatment for rand() function. reset the seed
	Func_rand* rand = dynamic_cast<Func_rand*>(fFunctor);
	if (rand)
		rand->seedSet(false);
}

bool FunctionColumn::operator==(const FunctionColumn& t) const
{
	// this is not being used. compilation error.

	const ReturnedColumn *rc1, *rc2;
	FunctionParm::const_iterator it, it2;

	rc1 = static_cast<const ReturnedColumn*>(this);
	rc2 = static_cast<const ReturnedColumn*>(&t);
	if (*rc1 != *rc2)
		return false;
	if (fFunctionName != t.fFunctionName)
		return false;
	if (fFunctionParms.size() != t.fFunctionParms.size())
		return false;
	for (it = fFunctionParms.begin(), it2 = t.fFunctionParms.begin();
		 it != fFunctionParms.end();
		 ++it, ++it2)
		if (**it != **it2)
			return false;
//	if (fAlias != t.fAlias)
//		return false;
	if (fTableAlias != t.fTableAlias)
		return false;
	if (fData != t.fData)
		return false;
	return true;
	return false;
}

bool FunctionColumn::operator==(const TreeNode* t) const
{
	const FunctionColumn *o;

	o = dynamic_cast<const FunctionColumn*>(t);
	if (o == NULL)
		return false;
	return *this == *o;
}

bool FunctionColumn::operator!=(const FunctionColumn& t) const
{
	return (!(*this == t));
}

bool FunctionColumn::operator!=(const TreeNode* t) const
{
	return (!(*this == t));
}

bool FunctionColumn::hasAggregate()
{
	if (fHasAggregate) return true;
	fAggColumnList.clear();
	for (uint32_t i = 0; i < fFunctionParms.size(); i++)
		fFunctionParms[i]->walk(getAggCols, &fAggColumnList);

	if (!fAggColumnList.empty())
		fHasAggregate = true;
	return fHasAggregate;
}

bool FunctionColumn::hasWindowFunc()
{
	fWindowFunctionColumnList.clear();
	for (uint32_t i = 0; i < fFunctionParms.size(); i++)
		fFunctionParms[i]->walk(getWindowFunctionCols, &fWindowFunctionColumnList);

	if (fWindowFunctionColumnList.empty())
		return false;
	return true;
}

void FunctionColumn::setDerivedTable()
{
	if (hasAggregate())
	{
		fDerivedTable = "";
		return;
	}

	setSimpleColumnList();
	string derivedTableAlias = "";
	for (uint32_t i = 0; i < fSimpleColumnList.size(); i++)
	{
		SimpleColumn* sc = fSimpleColumnList[i];
		sc->setDerivedTable();
		if (sc->derivedTable() != derivedTableAlias)
		{
			if (derivedTableAlias == "")
			{
				derivedTableAlias = sc->tableName();
			}
			else
			{
				derivedTableAlias = "";
				break;
			}
		}
	}
	fDerivedTable = derivedTableAlias;
}

void FunctionColumn::replaceRealCol(CalpontSelectExecutionPlan::ReturnedColumnList& derivedColList)
{
	for (uint i = 0; i < fFunctionParms.size(); i++)
	{
		ParseTree *pt = fFunctionParms[i].get();
		replaceRefCol(pt, derivedColList);
	}
}

void FunctionColumn::setSimpleColumnList()
{
	fSimpleColumnList.clear();
	for (uint i = 0; i < fFunctionParms.size(); i++)
		fFunctionParms[i]->walk(getSimpleCols, &fSimpleColumnList);
}

bool FunctionColumn::singleTable(CalpontSystemCatalog::TableAliasName& tan)
{
	tan.clear();
	setSimpleColumnList();
	for (uint i = 0; i < fSimpleColumnList.size(); i++)
	{
		CalpontSystemCatalog::TableAliasName stan(fSimpleColumnList[i]->schemaName(),
		                    fSimpleColumnList[i]->tableName(),
		                    fSimpleColumnList[i]->tableAlias(),
		                    fSimpleColumnList[i]->viewName());
		if (tan.table.empty())
			tan = stan;
		else if (stan != tan)
			return false;
	}
	return true;
}

}  //namespace
