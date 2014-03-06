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

/******************************************************************************************
* $Id: cpimport.cpp 33 2006-08-24 14:36:17Z wzhou $
*
******************************************************************************************/
#include <iostream>
#include <we_bulkload.h>

#define     ENV_BULK_DIR               "CP_BULK_DIR"        
#include <string>
#include <boost/progress.hpp>

using namespace std;
using namespace WriteEngine;

#define MAXSTRINGS 100000
string Lines[MAXSTRINGS];
typedef std::vector<std::string>          LineFldList;


   const int parseStr( const string& instr,  LineFldList fields) 
{
    typedef boost::tokenizer<boost::char_separator<char> > 
            tokenizer;
    boost::char_separator<char> sep("|");
    tokenizer tokens(instr, sep);
    for (tokenizer::iterator tok_iter = tokens.begin(); tok_iter != tokens.end(); ++tok_iter){
//        std::cout << "<" << *tok_iter << "> ";
        fields.push_back( *tok_iter );
    }
    //std::cout << "\n";
    return EXIT_SUCCESS;    

}
int strtok_test( const string& instr ){
    char *fragment;
    char *search = (char*)malloc( instr.length());
    
    memcpy( search, instr.c_str(), instr.length());
    
    fragment = strtok(search, "|");
    do {
       // printf("Token: %s\n", fragment);
        fragment = strtok(NULL, "|");
    } while (fragment);
    free( search );
    return EXIT_SUCCESS;
}

int handrolled_test( const string& instr ){
    char *search = (char*)malloc( instr.length());
    char *pos;
    int count=0;
    int span;
    string temp;
    string results[10];
    
    memcpy( search, instr.data(), instr.length());
    if (search[0] == '|'){ 
        pos = search+1;
    } else {
        pos = search;
    }    
    
    while (pos) {
        span = strcspn(pos, "|");
        if (span){
            temp.assign(pos, span);
            results[count++].assign(temp);
        }
        pos = index(pos+1, '|');
        if (pos){ pos++ ;}
    };
    free( search );
    //printf("\n%i dips", count);
    return EXIT_SUCCESS;
}   
    
int handrolled_test2( string& instr, string Fields[] ){

    char *search = (char*)malloc( instr.length() +1 );
    char *pos; // pos is used to step inside the search string
    int count=0; // keeps track of fields found
    int charspan;
    int num_bars;
    
    strcpy( search, instr.c_str() );
    pos = search;
    

    if (search[0] == '|'){ 
        pos = search+1;
        Fields[count++].assign(""); // a leading bar indicates an opening blank
    } else {
        pos = search;
    }    
    
    while (pos < search+instr.length()-1 ) {

        charspan = strcspn(pos, "|");
        if (charspan){
            Fields[count++].assign(pos, charspan);
            pos += charspan + 1;
        } else {
            Fields[count++].assign("");
            pos++;
        }
        
        num_bars = strspn(pos, "|");
        pos += num_bars;
            
        for( ; num_bars>0; num_bars--){
            Fields[count++].assign("");
        }
    };

    free( search );
    return count;
}   
 
int parseToken(){
    return 1;
}


int build_data(){
    int idx;
    for (idx=0; idx < MAXSTRINGS; idx++){
        //tpch data files are of the form
        // item|item|item and the line may end with |
        // even though this may wrongly suggest a blank value at the end 
        Lines[idx] = "12345|abcdef|banana|banana|";  // 'item item item item'
    }
    //std::cout  << Lines[idx-1] << endl;
    return 0;
}

int main(int argc, char **argv)
{
    
   string   sJobIdStr, sBulkDir = "", sDbDir = "", sFileName, sTmp;
   int fcount;
   string Fields[1000] ;
   string search;
   string searches[]= {
       "", "|", "|||||||||||||||", "12345|abcdef|banana|", "123456789012345678901234567890",
       "|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890|12345678901234567890",
       "|12345|abcdef|banana|bank123", "|123456789012345678901234567890", "12345|abcdef|banana|bank123",
       "12345||abcdef||banana|bank", "|12345||abcdef|banana|bank", "|12345|abcdef|banana|bank|",
       "|12345|abcdef|banana||", "|12345|abcdef|banana|||"
   };
   // 14 elements
   printf("\nAccuracy:");
   
 
   for (int test=0; test < 14; test++){
   printf("\n\nSearch string %i: %s", test, searches[test].c_str());
   fcount = handrolled_test2(searches[test], Fields);
   for (int idx = 0; idx < fcount; idx++){
       printf("\nString %i: %s$", idx, Fields[idx].c_str());
   }
   }
   
   printf("\n\nSpeed:\n");
   
   build_data();
   boost::timer t;
   
   LineFldList parseFields;
   for (int idx=0; idx< MAXSTRINGS; idx++){
       parseStr(Lines[idx], parseFields);
   }
   
   printf("Boost Parse Timer: %lf\n", t.elapsed());
   t.restart();
   
   for (int idx=0; idx< MAXSTRINGS; idx++){
       strtok_test(Lines[idx]);
   }
   printf("Strtok Timer: %lf\n", t.elapsed());

   t.restart();
   
   for (int idx=0; idx< MAXSTRINGS; idx++){
       handrolled_test(Lines[idx]);
   }
   printf("Handrolled Timer: %lf\n", t.elapsed());

   t.restart();
   
   for (int idx=0; idx< MAXSTRINGS; idx++){
       fcount = handrolled_test2(Lines[idx], Fields);
   }
   printf("Handrolled2 Timer: %lf\n", t.elapsed());
   
   printf("\n");
   return 0;
}





