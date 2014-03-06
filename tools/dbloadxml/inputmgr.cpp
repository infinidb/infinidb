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
* $Id: inputmgr.cpp 2258 2013-05-08 17:33:40Z dcathey $
*
*******************************************************************************/

#include "inputmgr.h"

#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <cerrno>

#include "calpontsystemcatalog.h"
#include "we_define.h"

using namespace std;
using namespace WriteEngine;
using namespace execplan;

namespace bulkloadxml
{

//------------------------------------------------------------------------------
// InputMgr constructor
//------------------------------------------------------------------------------
InputMgr::InputMgr(const string& job)
{ 
    fParms[ JOBID] = job;                       // add or override default value
}

//------------------------------------------------------------------------------
// InputMgr destructor
//------------------------------------------------------------------------------
/* virtual */
InputMgr::~InputMgr( )
{
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void InputMgr::printUsage()
{   //@bug 391
    cerr << "Usage: " << "colxml [options] dbName"  << endl << endl;
    cerr << "Options: "  << endl;
    cerr << "   -d Delimiter (default '|')\n";
    cerr << "   -e Maximum allowable errors (per table)\n";
    cerr << "   -h Print this message\n";
    cerr << "   -j Job id (numeric)\n";
    cerr << "   -l Load file name\n";
    cerr << "   -n \"name in quotes\"\n";
    cerr << "   -p Path for XML job description file that is generated\n";
    cerr << "   -s \"Description in quotes\"\n";
    cerr << "   -t Table name\n" ;
    cerr << "   -u User\n";
    cerr << "   -r Number of read buffers\n";
    cerr << "   -c Application read buffer size (in bytes)\n";
    cerr << "   -w I/O library buffer size (in bytes), used to read files\n";
    cerr << "   -x Extension of file name (default \".tbl\")\n";
    cerr << "   -E EnclosedByChar (if data has enclosed values)\n";
    cerr << "   -C EscapeChar\n";
    cerr << "   -b Debug level (1-3)\n\n";
    cerr << "   dbName - Required parm specifying the name of the database;"<<
            endl << "            all others are optional\n\n" ;
    cerr << "Example:\n\t" << "colxml -t lineitem -j 123 tpch\n";
    //    exit(0);
}

//------------------------------------------------------------------------------
// Provide input to this class through argc/argv interface.
//------------------------------------------------------------------------------
bool InputMgr::input(int argc, char **argv)
{
    std::vector<std::string> l_tableList;
    int ch;

    while( (ch=getopt(argc,argv,"b:d:s:j:l:e:n:p:t:u:r:c:w:x:hE:C:")) != EOF )
    {
        switch(ch)
        {
            case 't':
            {
                l_tableList.push_back(optarg);
                break;
            }

            case 'l':
            {
                fLoadNames.push_back(optarg);
                break;
            }

            case 'r':
            case 'c':
            case 'w':
            case 'j':
            case 'b':
            case 'e':
            {
                if (verifyArgument(optarg) < 0) 
                {
                    cout << "Argument associated with option -" <<
                        (char)ch << " is not a number." << endl; 
                    return false;
                }

                errno = 0;
                long lValue = strtol(optarg, 0, 10);
                if (errno != 0)
                {
                    cout << "Option -" << (char)ch <<
                        " is invalid or out of range" << endl;
                    return false;
                }

                // Limit to INT_MAX because we eventually store in an "int"
                if ( ((ch == 'e') && (lValue < 0)) ||
                     ((ch != 'e') && (lValue < 1)) ||
                     (lValue > INT_MAX))
                {
                    cout << "Option -" << (char)ch <<
                        " is invalid or out of range." << endl;
                    return false;
                }
            }

            case 'd':
            case 's':
            case 'f':
            case 'n':
            case 'p':
            case 'u':
            case 'x':
            case 'E':
            case 'C':
            {
                char l_option[4];
                snprintf(l_option,sizeof(l_option),"-%c",ch);
                ParmList::iterator p = fParms.find(l_option);
                if ( fParms.end() != p  )
                    p->second = optarg;
                break;
            }

            case 'h':
            {
                printUsage();
                return false;
            }

            default :
            {
                cout << "Try '" << argv[0] <<
                    " -h' for more information."<< endl;  
                return false;
            }
        }
    }

    if (optind < argc) 
    {
        fSchema = argv[optind++];
        if ( optind < argc ) 
        {
            cout << "Extraneous arguments are ignored." << endl;
        }
    }
    else 
    {
        cout << "Schema value is required." << endl;
        cout << "Try '" << argv[0] << " -h' for more information."<< endl;  
        return false;
    }
    
    for (unsigned ndx = 0; ndx < l_tableList.size(); ndx++)
    {
        fTables.push_back(execplan::CalpontSystemCatalog::TableName(
            fSchema, l_tableList[ndx]));
    }

    return true;
}
  
//------------------------------------------------------------------------------
// Get list of tables for fSchema, and store the list in fTables.
//------------------------------------------------------------------------------
bool  InputMgr::loadCatalogTables()
{
    boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatPtr =
        execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
        BULK_SYSCAT_SESSION_ID);
    systemCatPtr->identity(execplan::CalpontSystemCatalog::EC);
    const vector< pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> > tables 
            = systemCatPtr->getTables( fSchema );
    for (vector<pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> >::const_iterator it = tables.begin();
         it != tables.end(); ++it)
    {
        fTables.push_back((*it).second);
    }

    return (! fTables.empty());
}

//------------------------------------------------------------------------------
// Verify that string argument is numeric
//------------------------------------------------------------------------------
int InputMgr::verifyArgument(char *arg)
{
    while(*arg != 0)
    {
        if (!isdigit(*arg))
        {
          return -1;
        }
        arg++;
    }
    return 0;
}

//------------------------------------------------------------------------------
// print contents to specified stream
//------------------------------------------------------------------------------
/* virtual */
void InputMgr::print(std::ostream& os) const
{
    os << *this;
}

//------------------------------------------------------------------------------
// operator<< for InputMgr
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const InputMgr& m)
{
    os << "\n\tSchema:\t" << m.fSchema << "\n\t"   << "Tables:\t";

    for ( InputMgr::TableList::const_iterator i = m.fTables.begin();
          i != m.fTables.end(); ++i)
        os << i->table << "  ";

    os << "\n\t" << "Load Files: ";
    for ( InputMgr::LoadNames::const_iterator i = m.fLoadNames.begin();
          i != m.fLoadNames.end(); ++i)
        os << *i << "  ";

    for (InputMgr::ParmList::const_iterator i  = m.fParms.begin();
        i != m.fParms.end(); ++i)
    {
        // Don't report the enclosedBy if not enabled by user
        if ((i->first == XMLGenData::ENCLOSED_BY_CHAR) &&
            (i->second.length() < 1))
                continue;

        os << "\n\t" << i->first << "\t" << i->second;
    }

    os << std::endl;
    return os;
}

}  //namespace bulkloadxml
