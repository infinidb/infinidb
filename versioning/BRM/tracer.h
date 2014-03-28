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

/******************************************************************************
 * $Id: tracer.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class DBRM
 */

#ifndef TRACER_H_
#define TRACER_H_

#include <sys/types.h>
#include <string>
#include <vector>
#include <stdint.h>

#if defined(_MSC_VER) && defined(xxxTRACER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

/** @brief Trace beginning and end of a call.
*
* When turned on, fDebug is true, Tracer writes informational messages to /var/log/Calpont/brm.log.
* It keeps track of integral, string and boolean inputs, and integral and boolean outputs.
* If it should print a message immediately from the constructor, call it with the final parameter,
* writeNow, true (default).  If input and output parameters need to be added, call it with
* writeNow false.  Then writeBegin() must be called explicitely. 
*
* Input parameters are added with addInput, output with addOutput.
* 
* On end, if debug is set, an end message is printed with the output parameters. 
*/
class Tracer {
	public:
		/** @brief Tracer Constructor
		 *
		 * @param file (in string) the file name.
		 * @param line (in int ) the line number.  
		 * @param msg (in string) the message that will be printed at the begin and end.
		 * @param debug (in bool) if information should be printed
		 * @param writeNow (in bool) whether to print the begin message from the constructor
		 * 		(printed if debug is true.)
		 */
		EXPORT Tracer(const std::string& file, int line, const std::string& msg, bool debug, bool writeNow = true);

		/** @brief Tracer destructor
		 *
		 * Prints the output message if debug is true.
		 */		
		EXPORT ~Tracer();

		/** @brief writeBegin
		 *
		 * Prints the begin message in form:
		 * file@line number begin -  followed by the input parameters:
		 * name: (value)
		 */		
		EXPORT void writeBegin();

		/** @brief writeEnd
		 *
		 * Prints the end message in form:
		 * file@line number begin -  followed by the output parameters:
		 * name: (value)
		 */		
		EXPORT void writeEnd();

		/** @brief writeDirect
		 *
		 * Prints specified string immediately (directly) to the brm log.
		 * Different than the addInput and addOutput functions that
		 * accumulate the data for later printing to the brm log.
		 * @param msg String to be printed to the brm log
		 */		
		void writeDirect(const std::string& msg);

		/** @brief addInput
		 *
		 * @param name (in string) the variable name.
		 * @param value (in int* or string* or bool* ) the variable
		 */
		EXPORT void addInput(const std::string& name, const int* value);
		EXPORT void addInput(const std::string& name, const std::string* value);
		EXPORT void addInput(const std::string& name, const bool* value);
		EXPORT void addInput(const std::string& name, const short* value);
		EXPORT void addInput(const std::string& name, const int64_t* value);

		/** @brief addOutput
		 *
		 * @param name (in string) the variable name.
		 * @param value (in int* or bool* ) the variable  
		 */
		EXPORT void addOutput(const std::string& name, const int* value);
		EXPORT void addOutput(const std::string& name, const bool* value);
		EXPORT void addOutput(const std::string& name, const short* value);
		EXPORT void addOutput(const std::string& name, const int64_t* value);

	private:
		std::string timeStamp();
		typedef std::vector<std::pair <std::string, const int*> > ValuesVec;
		typedef std::vector<std::pair <std::string, const std::string*> > ValuesStrVec;
		typedef std::vector<std::pair <std::string, const bool*> > ValuesBoolVec;
		typedef std::vector<std::pair <std::string, const short*> > ValuesShortVec;
		typedef std::vector<std::pair <std::string, const int64_t*> > ValuesInt64Vec;
		std::string fFileName;
		int fLine;
		std::string fMsg;
		bool fDebug;
		ValuesVec fInputs;
		ValuesStrVec fStrInputs;
		ValuesBoolVec fBoolInputs;
		ValuesShortVec fShortInputs;
		ValuesInt64Vec fInt64Inputs;
		ValuesVec fOutputs;
		ValuesBoolVec fBoolOutputs;
		ValuesShortVec fShortOutputs;
		ValuesInt64Vec fInt64Outputs;
		pid_t fpid;
};

/** @brief tracer macros
*
* Use the contructors if there is a bool fDebug in scope.
*/


#define TRACER_WRITENOW(a)\
 	Tracer tracer(__FILE__, __LINE__, a, fDebug); 

#define TRACER_WRITELATER(a)\
 	Tracer tracer(__FILE__, __LINE__, a, fDebug, false); 

#define TRACER_ADDINPUT(a)\
 	tracer.addInput((std::string)#a, (int*)&a); 

#define TRACER_ADDSTRINPUT(a)\
 	tracer.addInput((std::string)#a, &a); 

#define TRACER_ADDBOOLINPUT(a)\
 	tracer.addInput((std::string)#a, &a); 

#define TRACER_ADDSHORTINPUT(a)\
 	tracer.addInput((std::string)#a, (short*)&a); 

#define TRACER_ADDINT64INPUT(a)\
 	tracer.addInput((std::string)#a, (int64_t*)&a); 

#define TRACER_ADDOUTPUT(a)\
 	tracer.addOutput((std::string)#a, (int*)&a); 

#define TRACER_ADDBOOLOUTPUT(a)\
 	tracer.addOutput((std::string)#a, &a); 

#define TRACER_ADDSHORTOUTPUT(a)\
 	tracer.addOutput((std::string)#a, (short*)&a); 

#define TRACER_ADDINT64OUTPUT(a)\
 	tracer.addOutput((std::string)#a, (int64_t*)&a); 

#define TRACER_WRITE\
 	tracer.writeBegin(); 

#define TRACER_WRITEDIRECT(msg)\
 	tracer.writeDirect(msg); 
}

#undef EXPORT

#endif 

