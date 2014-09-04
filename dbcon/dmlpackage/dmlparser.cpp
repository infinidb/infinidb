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
 *   $Id: dmlparser.cpp 8707 2012-07-13 19:08:12Z rdempsey $
 *
 *
 ***********************************************************************/
#include <fstream>
#include <errno.h>

#include "dmlparser.h"

#undef DECIMAL
#undef DELETE
#undef IN
#ifdef _MSC_VER
#include "dml-gram-win.h"
#else
#include "dml-gram.h"
#endif

#include <stdio.h>
#include <string.h>

int dmlparse();

namespace dmlpackage
{
    using namespace std;

	void scanner_finish(void);
    void scanner_init(const char *str);
    void grammar_init(dmlpackage::ParseTree* _ptree, bool);
    valbuf_t get_valbuffer(void);
  
    void free_copybuffer();
	void set_defaultSchema(std::string schema);

    DMLParser::DMLParser() :
    fStatus(-1), fDebug(false)
        {}

    DMLParser::~DMLParser()
    {
        scanner_finish();
    }

    void DMLParser::setDebug(bool debug)
    {
        fDebug = true;
    }

    int DMLParser::parse(const char* dmltext)
    {
        scanner_init(dmltext);
        grammar_init(&fParseTree, fDebug);
        fStatus = dmlparse();
        if (fStatus == 0)
        {
            char* str;
            valbuf_t valueBuffer = get_valbuffer();

            for(unsigned int i=0; i < valueBuffer.size(); i++)
            {
                str = valueBuffer[i];
                if(str)
                {
                    if (i > 0)
                        fParseTree.fSqlText += " ";
                    fParseTree.fSqlText += str;
                }
            }
        }
        free_copybuffer();
        return fStatus;
    }

    const ParseTree& DMLParser::getParseTree()
    {
        if (!good())
        {
            throw logic_error("The ParseTree is invalid");
        }
        return fParseTree;

    }

    bool DMLParser::good()
    {
        return fStatus == 0;
    }

	void DMLParser::setDefaultSchema(std::string schema)
	{
		set_defaultSchema(schema);
	}

    DMLFileParser::DMLFileParser()
        :DMLParser()
        {}

    int DMLFileParser::parse(const string& fileName)
    {
        fStatus = -1;

        ifstream ifdml;
        ifdml.open(fileName.c_str());
        if (!ifdml.is_open())
        {
            perror(fileName.c_str());
            return fStatus;
        }
        char dmlbuf[1024*1024];
        unsigned length;
        ifdml.seekg(0, ios::end);
        length = ifdml.tellg();
        ifdml.seekg(0, ios::beg);
        if (length > sizeof(dmlbuf) - 1)
        {
            throw length_error("DMLFileParser has file size hard limit of 16K.");
        }

        unsigned rcount;
        rcount = ifdml.readsome(dmlbuf, sizeof(dmlbuf) - 1);
        if (rcount < 0)
            return fStatus;

        dmlbuf[rcount] = 0;

       // cout << endl << fileName << "(" << rcount << ")" << endl;
        //cout << "-----------------------------" << endl;
        //cout << dmlbuf << endl;

        return DMLParser::parse(dmlbuf);
    }

    void end_sql(void)
    {

    }                                             /* end_sql */

}                                                 // dmlpackage
