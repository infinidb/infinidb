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
*   $Id: orasession.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef OCI_ORACLE
#include <oci.h>
#endif
#ifndef ODCI_ORACLE
#include <odci.h>
#include <string>
#endif

#include <string>
namespace plsql
{

/** @brief wraps an Oracle session
  * This class provides an Oracle session interface allowing a session to be started, DML statements
  * to be executed, and closing the session.
  */
class OraSession {

	public:

		/** @brief starts the session.
		*/
		void startSession(const std::string& sConnectAsUser, const std::string& sConnectAsUserPwd,
                             const std::string& sConnectDBLink);

		/** @brief issues a DML statement, a commit, or a rollback.
		*/
		void issueStatement(const std::string& stmt);

		/** @brief gets the password for a user (user must have select right to sys.dba_users).
		*/
		std::string getPasswordForUser(const std::string& user);

		/** @brief ends the session.
		*/
		void endSession();


		/** @brief returns the connected user or empty string if a session is not active.
		*/
		std::string getConnectedUser();

		/** @brief constructor
		*/
		OraSession();
		
	private:

		/* OCI Handles */
		/* Private struct used here instead of the one in checkerr.h for two reasons:
		   1) This is run in it's own process, so no external proc context pointer.
		   2) I was running into problems with corrupt pointers when using the Handles_t
		      with the OCIServer pointer and OCIStmt pointer as separate private members. */ 
		struct OCIHandles_t_
		{
			OCIServer *srvhp;
			OCIStmt *stmthp;
			OCIEnv* envhp;
			OCISvcCtx* svchp;
			OCIError* errhp;
			OCISession* usrhp;
			OCIHandles_t_():srvhp(0), stmthp(0), envhp(0), svchp(0), errhp(0), usrhp(0){}

			private:
			OCIHandles_t_(const OCIHandles_t_& rhs); 
			OCIHandles_t_& operator=(const OCIHandles_t_& rhs);

		};
		typedef struct OCIHandles_t_ OCIHandles_t;
		OCIHandles_t ociHandles;
		int checkerr(OCIHandles_t* handles, sword status, const char* info);
		inline int checkerr(OCIHandles_t* handles, sword status);
		OraSession(const OraSession& rhs); // private ctor
		OraSession& operator=(const OraSession& rhs);
		std::string connectedUser;
};	
		
		
} //namespace plsql


