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

/* $Id: planutils.cpp 9210 2013-01-21 14:10:42Z rdempsey $ */

/** @brief convert oracle execution plan to Calpont execution plan
 * Call calpont.cal_get_explain_plan stored function of Oracle to get
 * the oracle execution plan. Then convert it to Calpont execution plan
 * structure.
 */

// test execution plan on mysql database before Calpont DB engine is ready
#define MYSQL_TEST 0

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <stack>
#include <vector>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/any.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost;

#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "expressionparser.h"
#include "constantcolumn.h"
#include "treenode.h"
#include "operator.h"
#include "arithmeticcolumn.h"
#include "aggregatecolumn.h"
#include "existsfilter.h"
#include "functioncolumn.h"
#include "selectfilter.h"
#include "objectreader.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
#include "treenodeimpl.h"
#include "constantfilter.h"
#include "calpontsystemcatalog.h"
#include <ext/hash_map>
using namespace execplan;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;
using namespace __gnu_cxx;
#include "planutils.h"

/** Debug macro */
#define _DEBUG 1
namespace {
ofstream planlog("/tmp/plan.log");
}
#if _DEBUG
#define DEBUG planlog
#else
#define DEBUG if (false) planlog
#endif

bool checkFilter = false;

/** @brief plan record structure
 */
struct PlanRecord
{
    char operation[4000];
    char options[4000];
    char object_type[4000];
    char other[4000];
    char object_owner[4000];
    char search_columns[4000];
    char projection[4000];
    char object_name[4000];
    char alias[4000];    
    char extended_information[4000];    
    char access_predicates[4000];
    char filter_predicates[4000];
    char select_level[4000];
    char parent_id[4000];
    char id[4000];
    char card[4000];
};

struct State
{
  int state;
  char filter;
};
struct EndState
{
  int  endState;
  int  funcIndex;//0 is do nothing, 1 is add, 2 is delete
};

typedef vector<State>    VState;
typedef vector<EndState> VEndState;

// Hash func for State
template<class T> struct stateHash { };
template<> struct stateHash<State>
{
      size_t operator()(State state) const
      {
        unsigned long _h = 0;
        _h = state.state * 7 + state.filter;
        return _h;
      }
};


struct eqState
{
        bool operator() (const State s1, const State s2) const
        {
            if ((s1.state == s2.state) &&( s1.filter == s2.filter))
            {
               return true;
            }
            else
             return false;
        }
};
typedef hash_map<State, EndState, stateHash<State>, eqState> StateMap;
//typedef hash_map<int, int, hash<int>, eqInt> IntMap;

State state[] = { {0,'('},{0,'&'},{0,'|'},{0,'N'} , {0,'T'},
                        {1,'&'}, {1,'|'},{1,')'},
                        {2, '('}, {2,'N'}, {2, 'T'}, {2, '&'}, {2, '|'},{2,'X'},
                        {3, '('}, {3, 'T'},{3, 'N'}, {3, 'X'},
                        {4, ')'},{4, '&'}, {4, '|'},
                        {5, '&'},{5,'|'},{5,')'},
                        {6, 'N'}, {6, 'T'},{6, '('}, {6, 'X'},
                        {7, 'N'}, {7, 'T'},{7, '('}, {7, 'X'}
                };

EndState endState[] = { {2, 1},{0, 0}, {0,0}, {0, 0} , {1, 1},
                                    {3, 1}, {3, 1},{5,1},
                                    {2, 1}, {2, 0}, {4, 1}, {2, 0}, {2, 0}, {1,2},
                                    {2, 1}, {1, 1},{1, 2}, {1, 2},
                                    {5, 1},{6, 1}, {6,1},
                                    {7,1}, {7,1},{5,1},
                                    {4, 2}, {4, 1},{2, 1},{4,2},
                                    {5, 2}, {1, 1},{2, 1},{5,2}
                       };
//initialize the vector with the array
const size_t stateSize = sizeof state / sizeof(State); 
         
VState    vStartState(state, state + stateSize);
VEndState vEndState(endState, endState + stateSize);
StateMap  stateMap;

typedef struct PlanRecord PlanRecord;

typedef vector<PlanRecord> Records;

/** @brief <columnName, SimleColumn*> map
 * use multimap because same column name may appeare in different level select
 */
typedef execplan::CalpontSelectExecutionPlan::ColumnMap ColumnMap;

typedef map<execplan::CalpontSystemCatalog::TableName, int> TableMap;

// Flag to show consistant operator
enum OperatorFlag
{
    INIT = 0,   // init state or no constant operator available
    AND,
    OR
};

typedef map<string, execplan::Filter*> ConstFilterMap;

typedef stack<TreeNode*> TreeNodeStack;

typedef map<string, string> ViewMap;


namespace {
const int MAXROWS = 100;
Records records;
ColumnMap& colMap = CalpontSelectExecutionPlan::colMap();

string schemaName;
string tableName;
string aliasName;
string indexName;
      
bool tableAccessFlag;
ReturnedColumn* leftCol = 0;
int sequence = 0;
int windowSequence = -2;

TableMap tableMap;

OperatorFlag opflag = INIT;

ConstFilterMap constFilterMap;

ViewMap viewMap;

} //anon namespace

namespace plsql {
namespace planutils {

OCIExtProcContext* myCtx;
int sessionid = 0;
OCIError *errhp;
char curschema[80];
bool andFlag = false;
int  m_state = -1;

/** Optimization. Combine col/const filters whose columns are the same and the
 *  connection operators are consistant (AND, OR). To come up with a perfect
 *  algorithm to cover all the possible cases is not trivil, especially when
 *  involving all mixed operators and parenthesis. This solution is simplier and
 *  made of two parts: (1)When building the filter tree, check if one Oracle
 *  filter list has the same operator (AND or OR). If same, do the optimization.
 *  If not, skip. That can cover most cases include between and in predicate,
 *  even when qualified filters are not next to each other (but on one oracle
 *  plan step). (2)When walking the filter tree by an extra step of getplan),
 *  try to combine the left out qualified filters by merging operands under one
 *  operator.
 *           AND                     AND                      AND
 *           / \                     /  \                     /  \
 *          A   OR                  AND  C                   A    C
 *              / \                 / \
 *             C   C               A   C
 *  The above left tree is easy to handle by (2), the above middle tree is easy to
 *  handle by (1). Both of the two trees should be converted to the rightmost tree.
 *  Thus the combined soluction can handle above 90% optimization in a rather easy
 *  way.
 */
int stateFunc(const char a)
{
  EndState endState;
  State    curState;
  int      funcFlag;
  curState.state  = m_state;
  curState.filter = a;
  if (stateMap.find(curState)!= stateMap.end())
  {
     endState = (stateMap.find(curState))->second;
     m_state= endState.endState;
     funcFlag = endState.funcIndex;
  }
  else
  {
     m_state= -1;
     funcFlag = -1;
  }
  return funcFlag;
}

void combine(ParseTree* n, void *stack)
{
    TreeNodeStack *treeNodeStack = static_cast<TreeNodeStack*>(stack);
    if (typeid(*(n->data())) != typeid(Operator))
        treeNodeStack->push(n->data());
    else
    {
        Operator *op = dynamic_cast<Operator*>(n->data());
        if (treeNodeStack->size() == 0) return;
        if (treeNodeStack->size() == 1)
        {
            treeNodeStack->pop();
            return;
        }
        Filter *rf = dynamic_cast<Filter*>(treeNodeStack->top());
        treeNodeStack->pop();
        Filter *lf = dynamic_cast<Filter*>(treeNodeStack->top());
        treeNodeStack->pop();
        if (!rf || !lf) return;
        if (rf != n->right()->data() || lf != n->left()->data()) return;

        ConstantFilter *cf = dynamic_cast<ConstantFilter*>(rf->combinable(lf, op));

        // re-evaluate the operator
        if (!cf) return;

        treeNodeStack->push(cf);
        n->data(cf);
        n->left(NULL);
        n->right(NULL);
    }
}

/** @brief check error of OCI call */
void checkerr(OCIError* errhp, sword status)
{
    const size_t ErrBufSize = 2000;
    char errbuf[ErrBufSize];
    sb4 errcode;
    switch (status)
    {
    case OCI_SUCCESS:
        return;
    case OCI_SUCCESS_WITH_INFO:
        sprintf(errbuf, "Error - OCI_SUCCESS_WITH_INFO\n");
        break;
    case OCI_NEED_DATA:
        sprintf(errbuf, "Error - OCI_NEED_DATA\n");
        break;
    case OCI_NO_DATA:
        sprintf(errbuf, "Error - OCI_NO_DATA\n");
        break;
    case OCI_ERROR:
        OCIErrorGet ((dvoid *) errhp, (ub4) 1, (text *) NULL, &errcode,
          (text*)errbuf, (ub4)ErrBufSize, (ub4)OCI_HTYPE_ERROR);
        break;
    case OCI_INVALID_HANDLE:
        sprintf(errbuf, "Error - OCI_INVALID_HANDLE\n");
        break;
    case OCI_STILL_EXECUTING:
        sprintf(errbuf, "Error - OCI_STILL_EXECUTE\n");
        break;
    case OCI_CONTINUE:
        sprintf(errbuf, "Error - OCI_CONTINUE\n");
        break;
    default:
        break;
    }
    errbuf[ErrBufSize - 1] = 0;
#ifdef STANDALONE
    DEBUG << errbuf << endl;
    exit(1);
#else
    if (myCtx)
        OCIExtProcRaiseExcpWithMsg(myCtx, 9999, (text*)errbuf, strlen(errbuf));
    DEBUG << errbuf << endl;
#endif
}

/** @brief get oracle execution plan from stored procedure */
void getPlanRecords(OCIStmt* stmthp)
{
    int numOfCol = 16;
    OCIDefine* defnp[numOfCol];
    char col[numOfCol][4000];
    int i;

    // @bug 159 fix. cleanup records vector from the previous query (especially when it failed)
    records.erase(records.begin(), records.end());
    
    for ( i = 0; i < numOfCol; i++)
    {
        checkerr (errhp, OCIDefineByPos ( stmthp, &defnp[i], errhp, i+1, (dvoid*)&col[i],
              4000, SQLT_STR, (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, OCI_DEFAULT));
    }

    DEBUG << "========== Oracle Execution Plan ==========" << endl;
    for ( i = 0; i < MAXROWS; i++)
    {
        if (OCIStmtFetch2 (stmthp, errhp, 1, OCI_FETCH_NEXT, (sb4) i, OCI_DEFAULT))
            break;
        DEBUG << "Operation: " << col[0] << endl;
        DEBUG << "Options: " << col[1] << endl;
        DEBUG << "Object_type: " << col[2] << endl;
        DEBUG << "Other: " << col[3] << endl;
        DEBUG << "Object_owner: " << col[4] << endl;
        DEBUG << "Search_columns: " << col[5] << endl;
        DEBUG << "Projection: " << col[6] << endl;
        DEBUG << "Object_name: " << col[7] << endl;
        DEBUG << "Alias: " << col[8] << endl;
        DEBUG << "Extended_information: " << col[9] << endl;
        DEBUG << "Access_predicates: " << col[10] << endl;
        DEBUG << "Filter_predicates: " << col[11] << endl;
        DEBUG << "Select_level: " << col[12] << endl;
        DEBUG << "Parent_id: " << col[13] << endl;
        DEBUG << "Id: " << col[14] << endl;
        DEBUG << "Cardinality: " << col[15] << endl;
        DEBUG << "--------------------" << endl;
        DEBUG.flush();

        // save the row to planRecord vector.
        PlanRecord record;
        strcpy(record.operation, col[0]);
        strcpy(record.options, col[1]);
        strcpy(record.object_type, col[2]);
        strcpy(record.other, col[3]);
        strcpy(record.object_owner, col[4]);
        strcpy(record.search_columns, col[5]);
        strcpy(record.projection, col[6]);
        strcpy(record.object_name, col[7]);
        strcpy(record.alias, col[8]);
        strcpy(record.extended_information, col[9]);
        strcpy(record.access_predicates, col[10]);
        strcpy(record.filter_predicates, col[11]);
        strcpy(record.select_level, col[12]);
        strcpy(record.parent_id, col[13]);
        strcpy(record.id, col[14]);
        strcpy(record.card, col[15]);
        records.push_back(record);
    }
    
    DEBUG << "\n========== Conversion Trace ==========" << endl;
}

/** @brief parse one column token to a column object
 *  @param colToken string to be parsed
 *  @param outerJoin flag reference
 *  @param index flag reference
 *  @return ReturnedColumn object pointer
 */
ReturnedColumn* getColumn (string& token, bool& outerJoinFlag, bool& indexFlag)
{
    // @bug 188 fix. ignore Oracle internal value tag :B. watch for conflict
    // make sure :B is not in quotes
    // @bug 648. move the logic from getFilter to here. make sure the dropped
    // filter columns are still the colmap.
    string::size_type bPos = token.find(":B", 0);
    string::size_type startpos = 1;
    string::size_type endpos = 1;
    bool bFlag = false;
    if (bPos != string::npos)
    {
        startpos = token.find("'", endpos+1);
        if (startpos == string::npos) bFlag = true;
        while (startpos != string::npos)
        {
            string::size_type endpos = token.find("'", startpos+1);
            if (endpos != string::npos && bPos > startpos && bPos < endpos)
            {
                break;
            }
            else
            {
                startpos = token.find("'", endpos+1);
                continue;
            }
        }
    }    

    if (bFlag) 
        return NULL;

    if (token.compare("$nso_col_1") == 0)
    {
        ColumnMap::iterator mapIter = colMap.find(token);
        if (mapIter != colMap.end())
            return mapIter->second.get()->clone();
        return NULL;
    }

    // recover '[', ']' to '(', ')'
    for (unsigned int i = 0; i < token.length(); i++)  
    {      
        if (token[i] == '[') token[i] = '(';
        if (token[i] == ']') token[i] = ')';
    }
    string::size_type pos;
    // rule out outer join notation (+)
    // if outer join, set outerJoinFlag to true. the other side column should
    // set the returnAll flag.
    if ((pos = token.find("(+)")) != string::npos)
    {
        token = token.substr(0, pos);
        outerJoinFlag = 1;
    }

    // concat '^'
    if (token.find("^") != string::npos)
    {
        return new ArithmeticColumn(token, sessionid);
    }
    
    // @bug 499
    if (token.compare ("NULL") == 0)
        return NULL;    
    
    // literal constant
    if ( token[0] == '\'' )
        return new ConstantColumn (token.substr(1, token.size()-2), ConstantColumn::LITERAL);

    // Arithmetic Expression or function
    string arithOper = "+-*/()";
    pos = token.find_first_of(arithOper);
    if (pos != string::npos)
    {
        string::size_type pos1;
        pos1 = token.find("DISTINCT", 0);
        if (pos1 != string::npos)
            // remove distinct keyword in aggregation function. not handled by engine now
            token.erase(pos1, 8);
        // special process for interval expression. make it a function
        if ((pos1 = token.find("INTERVAL", 0)) != string::npos)
        {
            token.insert(pos1+8, "(");
            token.append(")");
        }
        
        //@bug 404. Insert columns to colMap
#if 1        
        string::size_type i = 0;
        std::vector<string> identifierList;
        while (token[i])
	      {
	        if (isalpha(token[i]) || token[i] == '_' )
	          {
	              string identifier;
	              while (isalnum(token[i]) ||
	                   token[i] == '_' ||
	                   token[i] == '.' ) 
	              {
	                  identifier.push_back(token[i++]);
	              }    
	                identifierList.push_back( identifier );                        
	          }       
	        i++;
	      }
	      boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
	      CalpontSystemCatalog::TableName tablename;  
        tablename.schema =  schemaName;
        tablename.table = tableName;
        if ( tableName.compare("") != 0 )
        {
          CalpontSystemCatalog::RIDList ridList = csc->columnRIDs(tablename, true);
          CalpontSystemCatalog::RIDList::const_iterator rid_iter = ridList.begin();
          CalpontSystemCatalog::ROPair roPair;
          CalpontSystemCatalog::TableColName tableColName;
          std::vector<std::string> colNames; 
          while ( rid_iter != ridList.end() )
          {
              roPair = *rid_iter;
              tableColName = csc->colName( roPair.objnum );
              colNames.push_back( tableColName.column );                  
              ++rid_iter;
          }
        vector<string>::const_iterator iter = identifierList.begin();
        while ( iter != identifierList.end() )
        {
          string colName = *iter;
          // remove table name if there is
          string alias = "";
          if ((pos = colName.find(".", 0)) != string::npos)
          {
              alias = colName.substr(0, pos);
              colName = colName.substr(colName.find_last_of(".")+1);
          }
          ColumnMap::iterator mapIter = colMap.find(colName);
          ColumnMap::iterator last = colMap.upper_bound(colName);
          if ( mapIter == colMap.end() )//Not in colMap yet
          {
            //check whether the name is in the colnames of this table
            boost::algorithm::to_lower(colName);
          
            vector<std::string>::const_iterator colname_iter = colNames.begin();
            while ( colname_iter != colNames.end() )
            {
              std::string col;
              col = *colname_iter;
              if ( col.compare(colName) == 0 )
                break;
              ++colname_iter;
            }
            if ( colname_iter != colNames.end())
            {         
              to_upper(schemaName);
              to_upper(tableName);
              to_upper(colName);
              SimpleColumn *sc = new SimpleColumn (schemaName, tableName, colName, sessionid);
                SSC sscp(sc);
                colMap.insert(ColumnMap::value_type(colName, sscp));
              tableMap[make_table(schemaName, tableName)] = 1;
            }
          }
          iter++;
        }        
      }
#endif      
        // @bug 844. simplest form of aggregation function support.
        // @note the following code covers only the narrowest scope (Basically TPCH18).
        // A full coverage of all cases takes much more consideration and checkings. 
        // The effort is saved because Oralce is not likely to be our future connector.
        if (token.find("SUM(") == 0)
        {
            string parm = token.substr(token.find("(")+1, token.find(")")-token.find("(")-1);
            ColumnMap::iterator mapIter = colMap.find(parm);
            if (mapIter != colMap.end())
            {                
                SRCP srcp(mapIter->second);
                AggregateColumn *ac = new AggregateColumn("sum", srcp.get()->clone(), sessionid);
                ac->addProjectCol(srcp);
                // get group by column
                char tmp[400];
                strcpy (tmp, (char*)records[sequence-1].projection);
                char* tok = strtok(tmp, "^");
                while (tok != NULL)
                {
                    mapIter = colMap.find(tok);
                    if (mapIter != colMap.end())
                        ac->addGroupByCol(mapIter->second);
                    else
                    {
                        SimpleColumn *sc = new SimpleColumn (schemaName, tableName, tok, sessionid);
                        if (sc->oid() < 0)
                            delete sc;
                        else
                        {
                            SSC sscp(sc);
                            colMap.insert(ColumnMap::value_type(tok, sscp));
                            ac->addGroupByCol(sscp);
                        }
                    }
                    tok = strtok(NULL, "^");
                }
                
                return ac;
             }
        }
        return new ArithmeticColumn(token, sessionid);
    }

    // numeric constant
    if ( isdigit(token[0]) || token[0] == '+' || token[0] == '-' || token[0] == '.')
        return new ConstantColumn (token, ConstantColumn::NUM);

    // SimpleColumn
    // remove table name if there is
  	string alias = "";
    if ((pos = token.find(".", 0)) != string::npos)
    {
       alias = token.substr(0, pos);
       token = token.substr(token.find_last_of(".")+1);
    }

    // @bug 551 fix in map iterator
    ColumnMap::iterator mapIter = colMap.lower_bound(token);
    int keycount = colMap.count(token);
    SimpleColumn* candidateCol = NULL;  // the one whose sequence # is most close to the current
    
    for (int i = 0; i < keycount; i++, ++mapIter)
    {
        SRCP srcp(mapIter->second);
        SimpleColumn *sc = dynamic_cast<SimpleColumn*>(srcp.get());
        
        // check if same table
        // @bug 1349. check keycount to avoid alias mismatch.
        if (tableAccessFlag && sc->tableName().compare(tableName) != 0 && keycount >1)
            continue;
            
        // rule out lhs and rhs are same column with same alias 
        // (o_totalprice = o_totalprice). they are more likely to
        // be of different tableAlias
        if (sc->sameColumn(leftCol) && sc->tableAlias().compare(dynamic_cast<SimpleColumn*>(leftCol)->tableAlias()) == 0)
        {
            // dbug
            //DEBUG << dynamic_cast<SimpleColumn*>(leftCol)->tableAlias() << endl;
            //DEBUG << sc->tableAlias() << endl;
            if (candidateCol != NULL && i == (keycount-1))
            {
                // check indexname. need to indicate in simplefilter which column
                // is used by oracle for index. because both column could be picked
                // by oracle for index scan at earlier steps
                if ((indexName != "" && candidateCol->indexName().compare(indexName) == 0) ||
                    (indexName == "" && candidateCol->indexName() != ""))
                    indexFlag = true;
                
                // set cardinality for this column by tracking parent_id. 
                // @todo may need to add this routine before every return in this loop
                if (records[sequence].id != records[candidateCol->sequence()].parent_id)
                {
                    for (uint c = 0; c < records.size(); c++)
                    {
                        string proj = records[c].projection;
                        if (records[sequence].id == records[c].parent_id &&
                            proj.find(token) == 0)
                            candidateCol->cardinality(atoi(records[c].card));
                    }
                }
                return candidateCol->clone();
            }
            continue;
        }    
            
        /** @info the following code is to try to fill in the correct table alias to the 
            column. It's very messy and needs more investigation */
            
        // for bug 150
        if (alias.length() == 0 && leftCol != 0 && sc->sameColumn(leftCol) 
            && sc->tableAlias().compare(dynamic_cast<SimpleColumn*>(leftCol)->tableAlias()) != 0)
        {
            if ((indexName != "" && sc->indexName().compare(indexName) == 0) ||
                (indexName == "" && sc->indexName() != ""))
                indexFlag = true;
            return sc->clone();
        }
        
        // try to fix alias bug-150
        // alias != 0 [alias.col]
        if (alias.length() != 0 &&
            sc->tableAlias().substr(0, sc->tableAlias().find_first_of("@"))
                         .compare(alias) == 0)
        {
            // the same alias may appear on different select level. pick the one 
            // whose sequence# is closest to the current filter step.
            if (candidateCol == NULL || ((sequence - sc->sequence()) >= 0 && (sequence - sc->sequence()) < sequence - candidateCol->sequence()))
                candidateCol = sc;
        }
        
        // no aliasName on the plan step. check alias   
        // @bug 1015. double check alias to avoid mismatch     
        if ((aliasName.length() == 0 && alias.length() == 0)
                || (alias.length() != 0 && 
                   (sc->tableAlias().substr(0, sc->tableAlias().find_first_of("@")).compare(alias) == 0) && 
                    sc->tableAlias().compare(aliasName) == 0))
        {
            if (keycount > 1) checkFilter = true;
            if (candidateCol == NULL || ((sequence - sc->sequence()) >= 0 && (sequence - sc->sequence()) < sequence - candidateCol->sequence()))
                candidateCol = sc;
        }
        
        if (candidateCol != NULL && i == (keycount-1))
        {
            // check indexname. need to indicate in simplefilter which column
            // is used by oracle for index. because both column could be picked
            // by oracle for index scan at earlier steps
            if ((indexName != "" && candidateCol->indexName().compare(indexName) == 0) ||
                (indexName == "" && candidateCol->indexName() != ""))
                indexFlag = true;
            return candidateCol->clone();
        }
        
        if (alias.length() == 0 && tableAccessFlag 
            && sc->tableName().compare(tableName) == 0 &&
            (aliasName.length() == 0 || sc->tableAlias() == aliasName))
        {
            if ((indexName != "" && sc->indexName().compare(indexName) == 0) ||
                (indexName == "" && sc->indexName() != ""))
                indexFlag = true;
            return sc->clone();
        }
/*
        // no aliasName on the plan step. check alias
        if ((aliasName.length() == 0 && alias.length() == 0)
                || sc->tableAlias().compare(aliasName) == 0)
        {
            if ((indexName != "" && sc->indexName().compare(indexName) == 0) ||
                (indexName == "" && sc->indexName() != ""))
                indexFlag = true;
            return sc->clone();
        }
*/
        // no alias for this column [col] but there's aliasName on this plan step.
        // matched column found if the tablename does not match. otherwise, go
        // to next column. this rule was added because bug150 fix broke the following
        // query: Select l_orderkey, l_partkey, o_orderdate, l_shipdate from orders,lineitem where o_orderdate between TO_DATE('1994-12-31', 'YYYY-MM-DD') and TO_DATE('1995-01-01', 'YYYY-MM-DD') and L_orderkey = O_orderkey
        // This is a little confusing and error prone. More investigation is
        // needed to make the rule more accurate. ??? seems not breaking comment query
        if (alias.length() == 0 && aliasName.length() != 0
            && sc->tableName().compare(tableName) != 0)
        {
            if ((indexName != "" && sc->indexName().compare(indexName) == 0) ||
                (indexName == "" && sc->indexName() != ""))
                indexFlag = true;
            return sc->clone();
        }   
        
        // alias = view name (subselect in FROM clause)
        if (alias.length() != 0 && tableName.length() == 0)
        {
            // find this entry in view table
            ViewMap::iterator viewIt = viewMap.find(alias);
            if (viewIt != viewMap.end())
            {
                //DEBUG << viewIt->first << " : " << viewIt->second << endl;
                if (sc->tableAlias().find(viewIt->second) != string::npos)
                    return sc->clone();
            }
        }

    }

    if (!tableAccessFlag )
       throw runtime_error ("Column " + token + " has not been accessed yet.");
    to_upper(schemaName);
    to_upper(tableName);
    to_upper(token);
    SimpleColumn *sc = new SimpleColumn (schemaName, tableName, token, sessionid);
    aliasName = (aliasName.length() == 0? tableName : aliasName);
    sc->tableAlias(aliasName);
    // make the index name complete if possible. this could be an indexed column
    // that appeared only on the filter but not the projection list. e.g. demo 4.
    sc->indexName(indexName);
    if (indexName != "" and sc->indexName().compare(indexName) == 0)
        indexFlag = true;
    sc->cardinality(atoi(records[sequence-1].card));
    SRCP srcp(sc);
    colMap.insert(ColumnMap::value_type(token, srcp));
    tableMap[make_table(schemaName, tableName)] = 1;
    sc->sequence(sequence);
    return sc->clone();
}

/** @brief parse one filter token to a filter object
 *  @param filterToken string to be parsed
 *  @return Filter object pointer
 */
Filter* getFilter (string& token)
{
    ReturnedColumn *lhs = 0, *rhs = 0;
    Operator *op;
    bool outerJoinFlagL = 0;
    bool outerJoinFlagR = 0;
    bool indexFlagL = 0;
    bool indexFlagR = 0;
    leftCol = 0;

    DEBUG << "TOKEN: <" << token << ">  " << endl;

    // between and in have been resolved as and / or
    string oper[] = {">=", "<=", "<>", "!=", "=", ">", "<", " NOT LIKE ", " LIKE ", " IS NULL", " IS NOT NULL", ""};

    for (int i = 0; oper[i].compare("") != 0; i++)
    {
        string::size_type lastPos = token.find(oper[i], 0);
        if (string::npos == lastPos)
            continue;

        // LHS
        string left = token.substr(0, lastPos);
        // @bug 631 VW in column name
        if (left.find("VW_") != string::npos && colMap.find(left) == colMap.end())
        {
            DEBUG << "Drop filter: " << token << endl;
            return NULL;
        }
        if (left.find("LNNVL") != string::npos && colMap.find(left) == colMap.end())
        {
            DEBUG << "Drop filter: " << token << endl;
            return NULL;
        }
        
        //if ( strcasecmp(records[sequence-1].options, "ANTI") == 0)
        //    return NULL;
        
        // RHS
        string right = token.substr(lastPos + oper[i].length());
        if (right.find("VW_") != string::npos && colMap.find(right) == colMap.end())
        {
            DEBUG << "Drop filter: " << token << endl;
            return NULL;
        }
        if (right.find("LNNVL") != string::npos && colMap.find(right) == colMap.end())
        {
            DEBUG << "Drop filter: " << token << endl;
            return NULL;
        }
        //if RHS token has table alias, get right column first
        // because it's easily matched. fix bug for log_key=ft.log_key case.
        if (left.find(".") == string::npos && right.find(".") != string::npos)
        {
        	if (right.length() != 0)
        	{
          		rhs = getColumn (right, outerJoinFlagR, indexFlagR);
          		leftCol = rhs; 	// @bug 150 fix         
          	}
          	lhs = getColumn (left, outerJoinFlagL, indexFlagL);
        }
        else
        {
        	lhs = getColumn (left, outerJoinFlagL, indexFlagL);
        	leftCol = lhs;   // @bug 150 fix  
        	if (right.length() != 0)
        		rhs = getColumn (right, outerJoinFlagR, indexFlagR);
        }
        
        // is null and is not null
        // @note need to pay more attention. "is null" != "= null"
        SOP sop;

        if ((oper[i].compare(" IS NULL") == 0) && (lhs != NULL))
        {
            sop.reset(new Operator("IS"));
            return new SimpleFilter (
                    sop,
                    lhs,
                    new ConstantColumn("", ConstantColumn::NULLDATA));
        }

        if ((oper[i].compare(" IS NOT NULL") == 0) && (lhs != NULL))
        {
            sop.reset(new Operator("IS NOT"));
            return new SimpleFilter (
                    sop,
                    lhs,
                    new ConstantColumn("", ConstantColumn::NULLDATA));
        }
	// @bug 761 move this after "IS NULL" checks
        // @bug 499 "NULL IS NOT NULL"
        // @bug 648
        if (!lhs || !rhs) 
        {
            DEBUG << "Drop filter: " << token << endl;
            return NULL;
        }
  
        // op
        op = new Operator (oper[i]);

#if 0  
        // work around for TE ad-hoc queries. self-join is not supported yet. remove 
        // self-join filter from the filter tree.
	    // @bug 598 Allow self-join
         SimpleColumn *tmpL = dynamic_cast<SimpleColumn*>(lhs);
         SimpleColumn *tmpR = dynamic_cast<SimpleColumn*>(rhs);
         if (tmpL && tmpR && (tmpL->tableName()).compare(tmpR->tableName()) == 0 && (tmpL->columnName()).compare(tmpR->columnName()) == 0)
         {
             DEBUG << "Self join filter:" << endl;
             DEBUG << *lhs << endl;
             DEBUG << *rhs << endl;
             return NULL;
         }
#endif
        
        lhs->returnAll(outerJoinFlagR);
        leftCol = 0;    // make leftCol null after right column processed
        outerJoinFlagR = 0;
        rhs->returnAll(outerJoinFlagL);
        
        // re-arrange column sides according to the access sequence. 
        // small sequence# on the left (smaller table as driven table)
        ReturnedColumn *tmp = 0;
        bool indexFlag = false;
                 
        if (typeid(*lhs) == typeid(SimpleColumn) &&
            typeid(*rhs) == typeid(SimpleColumn) &&
            lhs->sequence() > rhs->sequence())
        {
            tmp = lhs;
            lhs = rhs;
            rhs = tmp;
            indexFlag = indexFlagL;
            indexFlagL = indexFlagR;
            indexFlagR = indexFlag;
            Operator* newOp = op->opposite();
            delete op;
            op = newOp;
        }
        sop.reset(op);
        SimpleFilter *sf = new SimpleFilter(sop, lhs, rhs);
        if ( strcasecmp(records[sequence-1].options, "ANTI") == 0)
            sf->joinFlag (SimpleFilter::ANTI);
        if (indexFlagL && indexFlagR) sf->indexFlag(SimpleFilter::BOTH);
        else if (indexFlagL) sf->indexFlag(SimpleFilter::LEFT);
        else if (indexFlagR) sf->indexFlag(SimpleFilter::RIGHT);
        else sf->indexFlag(SimpleFilter::NOINDEX);
        return sf;
    }

    throw runtime_error ("Invalid filter token: " + token);
}

/** @brief break up the filter string into filters and populste the filterTokenList
 *  @param	filters filter string
 *        	filterTokenList the token vector to be populated
 */
void parseFilters (string& token, CalpontSelectExecutionPlan::FilterTokenList& list)
{  
    // bug#319 fix. oracle turns concat to ||
    string::size_type concatPos;
    bool notNullFlag=false;
    int funcFlag = 0;
    
    if ((concatPos = token.find("||")) != string::npos)
    {
        token.replace(concatPos, 2, "^");
    }
            
    // tokenize filters first
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
    boost::char_separator<char> sep("", "&|");
    tokenizer tokens(token, sep);
    int closePare, openPare, funcPare;
    closePare = openPare = funcPare = 0;
    SimpleFilter *sf = 0;   // only simplefilters are handled now

    // optimization. check consistant operator
    opflag = INIT;
    if (token.find("|") == string::npos)
    {
        if (token.find("&") != string::npos)
            opflag = AND;
    }
    else
    {
        if (token.find("&") == string::npos)
            opflag = OR;
    }

    // clean constFilterMap
    constFilterMap.erase(constFilterMap.begin(), constFilterMap.end());

    // force parenthesis
    list.push_back(new Operator("("));
    m_state = 0; //set up initial state
    int count =0;
    
    for (tokenizer::iterator tok_iter = tokens.begin();
        tok_iter != tokens.end(); ++tok_iter)
    {
        count++;
        if ((*tok_iter).compare("|") == 0 )
        {
			try {
                     funcFlag = stateFunc('|');
                     if (funcFlag == -1)
				         throw funcFlag;
                     if ( funcFlag == 1)
                          list.push_back(new Operator ("OR"));
                     else if (funcFlag == 2)
                    {
                          delete list.back();
                          list.pop_back();
                     }
			}
			catch (int flag)
		   {
                              cout << "state map is wrong, please fix it state =" 
                              <<  m_state << "funcFlag = " << flag 
                              << "filter = '|' " << endl;
                              exit(1);
            };
            continue;
        }

        if ((*tok_iter).compare("&") == 0)
        {
            try {
                    funcFlag = stateFunc('&');
					if (funcFlag == -1)
				         throw funcFlag;
                    if ( funcFlag == 1)
                       list.push_back(new Operator ("AND"));
                    else if (funcFlag == 2)
                   {
                         delete list.back();
                        list.pop_back();
                    }
			}
		   catch (int flag)
		   {
                     cout << "state map is wrong, please fix it state =" 
                            <<  m_state << "funcFlag = " << flag 
                            << "filter = '&' " << endl;
                     exit(1);
            };
            continue;
        }

        string temp = *tok_iter;

        // strip off leading and ending space
        string::size_type const first = temp.find_first_not_of(" ");
        temp = temp.substr(first, temp.find_last_not_of(" ")-first+1);
        
        //check for open and close parenthesis
        while (temp[0] == '(')
        {
            // @bug 241 fix. still open for watch more complicate cases
            // (-n_nationkey) < (-20). if '(' is followed immediately by '-'. 
            // replace the '(' with '[' and ')' with ']'                
            if (temp[1] == '-')
            {
                temp.replace(0, 1, "[");
                string::size_type position = temp.find_first_of(")");
                temp.replace(position, 1, "]");
                continue;
            }
            temp = temp.substr(1);
            // skip (, ) if opflag is set (no parenthesis necessary)
            if (!opflag)
            {
				try {
                       funcFlag = stateFunc('(');
					   if ( funcFlag == -1)
						   throw funcFlag;
                       if ( funcFlag == 1)
                              list.push_back(new Operator ("("));
                       else if (funcFlag == 2)
                       {
                              delete list.back();
                              list.pop_back();
                       }
				}
				catch (int flag)
		        {
                     cout << "state map is wrong, please fix it state =" 
                            <<  m_state << "funcFlag = " << flag 
                            << "filter = '(' " << endl;
                     exit(1);
                };
            }
            openPare++;
        }

        // check for function parenthesis to avoid confusion. leave function 
        // parenthesis untouched and getColumn will process it.
        string::size_type p = 0;
        while ((p = temp.find_first_of("(", ++p)) != string::npos)
            funcPare++;
        p = 0;
        while (funcPare && p != string::npos)
        {
            p = temp.find_first_of(")", ++p);
            funcPare--;
        }

        while (openPare && p < temp.length() && temp[temp.length()-1] == ')')
        {
            temp = temp.substr(0, temp.length()-1);
            closePare++;
            openPare--;
            funcPare++;
        }

        sf = dynamic_cast<SimpleFilter*>(getFilter(temp));
        if (sf)
        {
            sf->cardinality(atoi(records[sequence-1].card));            
            notNullFlag= true;
            SimpleColumn *leftCol = dynamic_cast<SimpleColumn*>
                                (const_cast<ReturnedColumn*>(sf->lhs()));
            // check col-const filter for optimization
            if (opflag && sf->pureFilter() && leftCol)
            {
                // constant filter. assume left operand is column (oracle makes sure)
                // check constant col map
                bool mergeFlag = false;
                string columnName = "";
                columnName = leftCol->columnName();
                    
                ConstFilterMap::iterator it = constFilterMap.find(columnName);
                ConstFilterMap::iterator last = constFilterMap.upper_bound(columnName);
                for (; it != constFilterMap.end() && it != last; ++it)
                {
                    // @note: Operations can still be merged on different table alias?
                    ConstantFilter *constFilter;
                    if (opflag == AND)
                        constFilter = dynamic_cast<ConstantFilter*>((*it).second->combinable(sf, new Operator("and")));
                    else    // should be 2 otherwise
                        constFilter = dynamic_cast<ConstantFilter*>((*it).second->combinable(sf, new Operator("or")));
                    if (constFilter)
                    {
                        mergeFlag = true;

                        // remove the last operator (become the extra one after combine) in filterTokenList
                        if ((typeid(*list.back()) == typeid(Operator)))
                        {
                            try
                            {
                                funcFlag = stateFunc('X');
                                if (funcFlag != 2)
                                   throw funcFlag;
                                delete list.back();
                                list.pop_back();
                            }
                            catch (int flag)
                            {
                                     cout << "state map is wrong, please fix it state =" 
                                    <<  m_state << "funcFlag = " << flag 
                                    << "filter = 'X' " << endl;
                                    exit(1);
                            };
                        }
                        break;
                    }
                }
                if (!mergeFlag)
                {
                    ConstantFilter *cf = new ConstantFilter(sf);
                    constFilterMap.insert(ConstFilterMap::value_type(cf->col()->columnName(), cf));
					 try
                    {
                           funcFlag = stateFunc('T');
						   if (funcFlag == -1)
							   throw funcFlag;
                           if (funcFlag==1)
                                list.push_back(cf);
                           else if (funcFlag ==2)
                           {
                                delete list.back();
                                list.pop_back();
                           }
					 }
					 catch (int flag)
					 {
						 cout << "state map is wrong, please fix it state =" 
                                 <<  m_state << "funcFlag = " << flag 
                                 << "filter = 'T' " << endl;
                         exit(1);
					 }
                }
            }
            else
            {
				try {
                         funcFlag = stateFunc('T');
				        if (funcFlag == -1)
					           throw funcFlag;
                        if (funcFlag ==1)
                               list.push_back (sf);
                        else if (funcFlag ==2)
                        {
                              delete list.back();
                              list.pop_back();
                        }
				}
				catch (int flag)
			   {
						 cout << "state map is wrong, please fix it state =" 
                                 <<  m_state << "funcFlag = " << flag 
                                 << "filter = 'T' " << endl;
                         exit(1);
			   }
            }
        }        
        // ignored filter. need to remove the extra operators
        else
        {
			try {
                funcFlag = stateFunc('N');
				if ((funcFlag == -1) || (funcFlag == 1))
					throw funcFlag;
                if (funcFlag == 2)
                {
                  delete list.back();
                  list.pop_back();
                }    
				
			}
			catch (int flag)
			{
						 cout << "state map is wrong, please fix it state =" 
                                 <<  m_state << "funcFlag = " << flag 
                                 << "filter = 'N' " << endl;
                         exit(1);
			}
        }
        while (!opflag && closePare)
        {
           try
           {
              funcFlag = stateFunc(')');
              if (funcFlag == -1)
                  throw funcFlag;
              if ( funcFlag == 1)
                  list.push_back(new Operator (")"));
              else if (funcFlag == 2)
             {
                  delete list.back();
                  list.pop_back();
             } 
           }
           catch (int flag)  
           {
              cout << "state map is wrong, please fix it state =" 
                   <<  m_state << "funcFlag = " << flag 
                   << "filer = ')' "<< endl;
              exit(1);
           }
           closePare--;
        }
        andFlag = notNullFlag;
    }
    //This is the end
    try
    {
      if ((m_state == 1) || (m_state == 5))
        list.push_back( new Operator (")"));
      else if (m_state == 0)
      {
                  delete list.back();
                  list.pop_back();
                  if (list.size() > 0) //previous filter exist then there is an and exist 
                  {
                     delete list.back(); //delete the previous and
                     list.pop_back();
                     andFlag = true;
                  } 
      }
      else
        throw m_state;
    }
    catch (int state)
    {
        cout << "m_state is not a final state = " << state << endl;
    }
}

/** @brief convert from oracle to calpont execution plan */
void doConversion(CalpontSelectExecutionPlan &csep)
{
    Records::const_iterator i;
    CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
    CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
    CalpontSelectExecutionPlan::GroupByColumnList groupByColumnList;
    bool orFlag = false;

    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

    // clean up before building plan
    colMap.erase(colMap.begin(), colMap.end());
    tableMap.erase(tableMap.begin(), tableMap.end());
    viewMap.erase(viewMap.begin(), viewMap.end());
    for (int i=0; i<(int)stateSize; i++)
          stateMap[vStartState[i]]= vEndState[i];         
    
    // 1st scan: get all the projections first to populate colMap before doing filters.
    for (sequence = 1, i = records.begin(); i != records.end(); i++, ++sequence)
    {
        // catch the projection for table and index access operations
        if (strcmp((*i).operation, "table access") == 0 ||
            strcmp((*i).operation, "index") == 0)
        {
            schemaName = (*i).object_owner;
            aliasName = string((*i).alias); 
            tableName = "";
            indexName = "";                               
            
            // the identify of object_name depends on the object_type of operation
            if (strcmp((*i).object_type,"TABLE") == 0)
                tableName = (*i).object_name;
            else if (strcmp((*i).operation,"index") == 0 && strcmp((*i).options, "FULL SCAN") != 0)
            {
                indexName = (*i).object_name;
                //@bug 357 Need to look up systables find out the real table name for index
                boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
                CalpontSystemCatalog::TableName tablename = csc->lookupTableForIndex( indexName, schemaName);
                tableName = tablename.table;
            }
                

            // e.g., aliasname: region@sel$1; alias: region. 
            // alias could be used as tablename if the latter is missing
            string alias = (aliasName.find_first_of("@") == string::npos?
                                tableName : aliasName.substr(0, aliasName.find_first_of("@")));
            tableName = (tableName.length() == 0? alias : tableName);
            
            // @bug 173 fixed. populate tablemap after (instead of before) empty tablename is fixed by alias
            to_upper(schemaName);
            to_upper(tableName);
            to_upper(aliasName);
            CalpontSystemCatalog::TableName tn = make_table(schemaName, tableName);
            tableMap[tn] = 0; 
            
            char tmp[4000];
            strcpy (tmp, (char*)(*i).projection);
            char* tok = strtok(tmp, "^");
            while (tok != NULL)
            {
                string colTok = tok;
                tok = strtok(NULL, "^");

                // TODO: need to translate this to a grouping operation. Calpont engine has to
                // support grouping for tpch queries (Q2). Oracle frontend will not do extra
                // grouping on returned result
                if (colTok.find("PARTITION BY") != string::npos ||
                    colTok.find("VW_COL") != string::npos || colTok.find("$nso") != string::npos)
                    continue;

                // remove table name if there is
                if (colTok.find(".", 0) != string::npos)
                    colTok = colTok.substr(colTok.find_last_of(".")+1);

                // ignore rowid and rownum identifier
                if (colTok.compare("ROWID") == 0 || colTok.compare("ROWNUM") == 0)
                    continue;
                
                // build simple column
                SimpleColumn *sc = new SimpleColumn(schemaName, tableName, colTok, sessionid);
                sc->tableAlias (aliasName);
                sc->indexName (indexName);

                // check map to make sure no same column already exist
                ColumnMap::iterator mapIter = colMap.find(colTok);
                ColumnMap::iterator last = colMap.upper_bound(colTok); 
                bool existFlag = false;
                               
                for (; mapIter != colMap.end() && mapIter != last; ++mapIter)
                {
                    SRCP srcp(mapIter->second);
                    SimpleColumn *col = dynamic_cast<SimpleColumn*>(srcp.get());

                    if (col->tableName().compare(sc->tableName()) == 0 &&
                        col->tableAlias().compare(sc->tableAlias()) == 0)
                    {
                        existFlag = true;
                        break;
                    }
                }
                if (!existFlag) 
                {
                    // mark access flag=1 for the table in tablemap
                    tableMap[make_table(sc->schemaName(), sc->tableName())] = 1;
                    // set access sequence number
                    sc->sequence(sequence);
                    sc->cardinality(atoi((*i).card));
                    SRCP srcp(sc);
                    colMap.insert(ColumnMap::value_type(colTok, srcp));
                }
            }
        }
        
        // populate a view map for view column filter
        if (strcmp((*i).operation, "view") == 0)
        {
            string viewAlias = string(i->alias).substr(0, string(i->alias).find_first_of("@"));
            viewMap.insert(ViewMap::value_type(viewAlias, (*i).select_level));
                
            // temp fix for tpch18. very limited coverage. try to map $nso_col_1 to a column
            if (strcmp((*i).projection, "$nso_col_1") == 0 )
            {
                string sel_level = (*i).select_level;
                for (int j = 0; j < sequence; j++)
                {
                    if (strcmp(records[j].select_level, sel_level.c_str()) == 0 &&
                        strcmp(records[j].operation, "table access") != 0)
                    {
                        string vwcol = records[j].projection;
                        string::size_type const first = vwcol.find_first_not_of(" ");
                        vwcol = vwcol.substr(first, vwcol.find_last_not_of(" ")-first+1);
                        ColumnMap::iterator mapIter = colMap.find(vwcol);
                        if (mapIter != colMap.end())
                        {
                            mapIter->second->cardinality(atoi((*i).card));
                            colMap.insert(ColumnMap::value_type(i->projection, mapIter->second));
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // 2nd scan: for filters and missed cols (cols that in filters but not in projection)
    for (sequence = 1, i = records.begin(); i != records.end(); i++, ++sequence)
    {
        schemaName = (*i).object_owner;
        // @bug #393 fix
        CalpontSelectExecutionPlan::schemaName(schemaName);
        aliasName = string((*i).alias);
        tableName = "";
        indexName = "";
        
        // if it's table or index access operation, sometimes oracle gives a filter
        // directly without project the columns. that implies the table or index name
        // for the filter columns. 
        if (strcmp((*i).operation, "table access") == 0 || strcmp((*i).operation, "index") == 0)        
        {
            tableAccessFlag = true;
            // fix index
            if (strcmp((*i).object_type,"TABLE") == 0)
                tableName = (*i).object_name;
            else if (strcmp((*i).operation,"index") == 0)
            {
                indexName = (*i).object_name;
                //@bug 357 Need to look up systables find out the real table name for index
                boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
                CalpontSystemCatalog::TableName tablename = csc->lookupTableForIndex( indexName, schemaName);
                tableName = tablename.table;
            }
            string alias = (aliasName.find_first_of("@") == string::npos?
                            tableName : aliasName.substr(0, aliasName.find_first_of("@")));
            tableName = (tableName.length() == 0? alias : tableName);
            // @bug #393 fix
            CalpontSelectExecutionPlan::tableName(tableName);
        }
        else
            tableAccessFlag = false;

        // get filter tree.
        if (strlen((*i).access_predicates) != 0)
        {
            string filters = (*i).access_predicates;

            if (filters.find("SELECT") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;
            }
                        
            if (filters.find("COUNT(*)") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }
            
            if (filters.find("CASE  WHEN") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }
            
            // fix for AH14. ignore window filters
            if (strcmp((*i).operation, "window") == 0 && filters.find(" OVER "))
            {
                windowSequence = sequence;
                DEBUG << "Drop filter: " << filters << endl;
                continue;                         
            }
            
            if (strcmp((*i).operation, "view") == 0 && (sequence = windowSequence+1))
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }

            // andFlag is to rule out only filter case. no and should be appended.
            if (orFlag)
            {
                filterTokenList.push_back (new Operator ("or"));
             }
            else if (andFlag)
            {
                filterTokenList.push_back (new Operator ("and"));
            }
            //andFlag = true;
            
            // try to fix the interruption of lnnvl. it's a quick fix and need to do more
            // more investigation and watch other conflict
            // the query is "select count(C_ACCTBAL) from CUSTOMER where C_CUSTKEY >= 100000 or (C_CUSTKEY <= 10 and c_custkey>1);" 
            // the environment is c_custkey is an index and the stats are feed right
            // check next step. if the filter predicate is lnnvl, then "or" the current and next
            // filters instead or "and".
            if (++i != records.end() && ((string)(*i).filter_predicates).find("LNNVL") != string::npos)            
                orFlag = true;            
            else
                orFlag = false;
            i--;
            parseFilters(filters, filterTokenList);
        }

        if (strlen((*i).filter_predicates) != 0)
        {
            string filters = (*i).filter_predicates;
            
            // @bug 755. drop hash join filter operation for now for TPCH19.
            string operation = (*i).operation;
            if (operation.find ("hash join") != string::npos){
                DEBUG << "Drop HJ filter: " << filters << endl;
                continue;
            }  
            
            // ignore filter_predicates if it's identical to access_predicates
            if (filters.compare((*i).access_predicates) == 0)
                continue;

            if (filters.find("SELECT") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }
            
            if (filters.find("COUNT(*)") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            } 

            if (filters.find("CASE  WHEN") != string::npos)
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }
                
            if (strcmp((*i).operation, "window") == 0 && filters.find(" OVER "))
            {
                windowSequence = sequence;
                DEBUG << "Drop filter: " << filters << endl;
                continue;    
            }
            
            if (strcmp((*i).operation, "view") == 0 && (sequence = windowSequence+1))
            {
                DEBUG << "Drop filter: " << filters << endl;
                continue;        
            }
            
            if (andFlag)
            {
                filterTokenList.push_back (new Operator ("and"));
            }      
            //andFlag = true;      
            parseFilters(filters, filterTokenList);
        }

// ignore group by list for now. not handled by engine
#if 0
        // get group by list
        if (strcmp((*i).operation, "sort") == 0 || strcmp((*i).options, "GROUP BY") == 0)
        {
            boost::char_separator<char> sep("^");
            string projection = (*i).projection;

            // sripp off leading 0, otherwise will trigger boost tokenizer bug
            projection = projection.substr(projection.find_first_not_of(" "));
            tokenizer tokens (projection, sep);
            //ColumnMap::iterator mapIter;
            for (tokenizer::iterator tok_iter = tokens.begin();
                 tok_iter != tokens.end(); ++tok_iter)
            {
                string tmp = *tok_iter;

                // ignore rowid and rownum
                if (tmp.compare("ROWID") == 0 || tmp.compare("ROWNUM") == 0)
                    continue;
                ReturnedColumn* rc = getColumn (tmp, outerJoinFlag);
                groupByColumnList.push_back (rc);
            }
            csep.groupByCols(groupByColumnList);
        }
#endif
    }

    // get returned column list. assume the second statement from the 
    // last gives the returned columns. ignore returned column for now
#if 0    
    boost::char_separator<char> sep("^");
    string projection = records[records.size()-2].projection;

    // sripe off leading 0, otherwise will trigger boost tokenizer bug
    projection = projection.substr(projection.find_first_not_of(" "));
    tokenizer tokens (projection, sep);

    for (tokenizer::iterator tok_iter = tokens.begin();
         tok_iter != tokens.end(); ++tok_iter)
    {
        string col = *tok_iter;
        returnedColumnList.push_back(getColumn(col, outerJoinFlag));
    }
    csep.returnedCols(returnedColumnList);
#endif

    // @bug 170 fix add at least one column for every table in tableMap that has access flag unset
    // to help performance, get the smallest size column
    // @bug 182 fix    
    TableMap::iterator tb_iter = tableMap.begin();
    for (; tb_iter != tableMap.end(); tb_iter++)
    {
        if ((*tb_iter).second == 1) continue;
        CalpontSystemCatalog::TableName tn = (*tb_iter).first;
        
        boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionid);
        CalpontSystemCatalog::RIDList oidlist = csc->columnRIDs(tn, true);
        CalpontSystemCatalog::TableColName tcn;
        int minColWidth = 0;
        int minWidthColOffset = 0;
        bool nullable = false;
        for (unsigned int j = 0; j < oidlist.size(); j++)
        {            
            CalpontSystemCatalog::ColType ct = csc->colType(oidlist[j].objnum);
            if (j == 0) 
            {
                minColWidth = ct.colWidth;
            }
            if (ct.constraintType == CalpontSystemCatalog::NOTNULL_CONSTRAINT) 
            {
                nullable = true;
                // @bug 556. add all notnull columns of the tables to colmap.
                tcn = csc->colName(oidlist[j].objnum);
                to_upper(tcn.schema);
                to_upper(tcn.table);
                to_upper(tcn.column);
                SimpleColumn *sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, sessionid);
                ColumnMap::const_iterator iter = colMap.find(tcn.column);
                SRCP srcp(sc);
                if (iter == colMap.end())
                    colMap.insert(ColumnMap::value_type(tcn.column, srcp));                
            }
            else if (!nullable && ct.colWidth < minColWidth)
            {
                minColWidth = ct.colWidth;
                minWidthColOffset= j;
            }            
        }           
        if (!nullable)
        {             
            tcn = csc->colName(oidlist[minWidthColOffset].objnum);
            to_upper(tcn.schema);
            to_upper(tcn.table);
            to_upper(tcn.column);
            SimpleColumn *sc = new SimpleColumn(tcn.schema, tcn.table, tcn.column, sessionid);
            SRCP srcp(sc);
            ColumnMap::const_iterator iter = colMap.find(tcn.column);
            if (iter == colMap.end())
                colMap.insert(ColumnMap::value_type(tcn.column, srcp));
        }
        (*tb_iter).second = 1;
    }
    
    // @bug 844. remove "$nso_col_1" from column map it exists
    ColumnMap::iterator miter = colMap.find("$nso_col_1");
    if (miter != colMap.end())
        colMap.erase(miter); 
    
    // tmp fix for Cindy's connector
    for (miter = colMap.begin(); miter != colMap.end(); miter++)
        if (static_cast<SimpleColumn*>(miter->second.get())->oid() < 0)
        {
            colMap.erase(miter);
            break;
        }   
    
    // @bug 960. recheck duplicate filter. this is temp fix. a more decent fix need to
    // get parent_id. from the oracle execution plan. too risky to change that for now. ZZ-07/10/08
    CalpontSelectExecutionPlan::FilterTokenList::iterator iter;
    pair<ColumnMap::iterator, ColumnMap::iterator> itPair;
    if (checkFilter)
    {
        vector<SimpleFilter*> simFilVec;
        vector<SimpleFilter*>::iterator si;
        for (iter = filterTokenList.begin(); iter != filterTokenList.end(); iter++)
        {
            SimpleFilter* sf = dynamic_cast<SimpleFilter*>(*iter);
            if (sf)
            {
                const SimpleColumn* lsc = dynamic_cast<const SimpleColumn*> (sf->lhs());
                const SimpleColumn* rsc = dynamic_cast<const SimpleColumn*> (sf->rhs());
                if (lsc == NULL || rsc == NULL)
                    continue;
                for (si = simFilVec.begin(); si != simFilVec.end(); ++si)
                {
                    if (lsc->sameColumn((*si)->lhs()) && rsc->sameColumn((*si)->rhs()))
                    {
                        // change the filter to refer to a difference alias
                        itPair = colMap.equal_range(lsc->columnName());
                        for (miter = itPair.first; miter != itPair.second; ++miter)
                        {
                            if (!lsc->sameColumn(miter->second.get()))
                                sf->lhs(dynamic_cast<SimpleColumn*>(miter->second.get())->clone());
                        }
                        itPair = colMap.equal_range(rsc->columnName());
                        for (miter = itPair.first; miter != itPair.second; ++miter)
                        {
                            if (!rsc->sameColumn(miter->second.get()))
                                sf->rhs(dynamic_cast<SimpleColumn*>(miter->second.get())->clone());
                        }                        
                    }
                }
                simFilVec.push_back(sf);
            }
        }
        simFilVec.clear();
    }
    // debug logging
    DEBUG << "\n========== Calpont DEBUG FILTER =========" << endl;
    
    for (iter = filterTokenList.begin(); iter != filterTokenList.end(); iter++)
        DEBUG <<  **iter   <<endl;
    // assign non-static column map for ExeMgr to use
    csep.columnMap(colMap);
    csep.filterTokenList(filterTokenList);

    // optimization: constant filter combine
    ParseTree* pt = const_cast<ParseTree*>(csep.filters());
    TreeNodeStack *treeNodeStack = new TreeNodeStack();
    if (pt)
    {
        pt->walk(combine, treeNodeStack);
        pt->drawTree("/tmp/filter1.dot");
    }

    ColumnMap::iterator j;
        
    DEBUG << "\n========== Projected Columns =========" << endl;
    for (j = colMap.begin(); j != colMap.end(); j++)
        DEBUG << (*j).first << ": " << *(*j).second << endl;
    
    DEBUG << "\n========== Calpont Execution Plan =========" << endl;
    DEBUG << csep << endl;
    andFlag = false;
    checkFilter = false;
}

} //namespace planutils
} //namespace plsql

