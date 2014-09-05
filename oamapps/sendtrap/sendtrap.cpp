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

/******************************************************************************************
* Author: Zhixuan Zhu
******************************************************************************************/
#define SNMPMANAGER_DLLEXPORT
#include "snmpmanager.h"
#undef SNMPMANAGER_DLLEXPORT

#include <cstdio>
#include <algorithm>
#include <vector>
#include <iterator>

#include "messagequeue.h"
#include "snmpglobal.h"
#include "liboamcpp.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace logging;
using namespace snmpmanager;


/******************************************************************************************
* @brief	main
*
* purpose:	call snmpapi to send traps and generate alarms
*
******************************************************************************************/
int main(int argc, char **argv)
{
	Oam oam;

	if (argc < 8 )
		exit (0);

	const char* componentID = argv[1];
	int alarmID = atoi(argv[2]);
	int state = atoi(argv[3]);
	string ModuleName = argv[4];
	string processName = argv[5];
	int pid = atoi(argv[6]);
	int tid = atoi(argv[7]);
	string parentOAMModuleName = argv[8];
	// Initialize Trap session
	{
		init_traps();
	
		// get IP address of Parent Module
		ModuleConfig Moduleconfig;
		string ipAdd;
		try {
			oam.getSystemConfig(parentOAMModuleName, Moduleconfig);
			HostConfigList::iterator pt1 = Moduleconfig.hostConfigList.begin();
			if (pt1 != Moduleconfig.hostConfigList.end())
				ipAdd = pt1->IPAddr;
			else
				ipAdd = "127.0.0.1";
		}
		catch (...) {
			ipAdd = "127.0.0.1";
		}

		// session initialization
		create_trap_session(ipAdd.c_str(), 
							SNMP_TRAP_PORT, 
							"public",
							SNMP_VERSION_1, 
							SNMP_MSG_TRAP);
	}
			
	/*
	 * define the OID for the notification we're going to send
	 * CALPONT-MIB::calpont-trap 
	 */
	const CALPONT_OID *notification_oid = CALPONT_TRAP_OID;
	size_t notification_oid_len = OID_LENGTH (CALPONT_TRAP_OID);
	
	/*
	 * In the notification, we have to assign our notification OID to
	 * the snmpTrapOID.0 object. Here is it's definition. 
	 */
	const CALPONT_OID *objid_snmptrap = SNMPTRAP_OID;
	size_t objid_snmptrap_len = OID_LENGTH (SNMPTRAP_OID);
	
	/*
	* define the OIDs for the varbinds we're going to include
	*  with the notification -
	* calpont-trap::calalarm_data
	* calpont-trap::componentID
	* calpont-trap::alarmID
	* calpont-trap::state
	* calpont-trap::sname
	* calpont-trap::pname
	* calpont-trap::pid
	* calpont-trap::tid
	*/
	const CALPONT_OID	   *calalarm_data_oid = CALALARM_DATA_OID;
	size_t	 calalarm_data_oid_len = OID_LENGTH(CALALARM_DATA_OID);
	const CALPONT_OID      *component_id_oid   = COMPONENT_ID_OID;
	size_t   component_id_oid_len = OID_LENGTH(COMPONENT_ID_OID);
	const CALPONT_OID      *alarm_id_oid   = ALARM_ID_OID;
	size_t   alarm_id_oid_len = OID_LENGTH(ALARM_ID_OID);
	const CALPONT_OID      *state_oid   = STATE_OID;
	size_t   state_oid_len = OID_LENGTH(STATE_OID);
	const CALPONT_OID      *sname_oid   = SNAME_OID;
	size_t   sname_oid_len = OID_LENGTH(SNAME_OID);
	const CALPONT_OID      *pname_oid   = PNAME_OID;
	size_t   pname_oid_len = OID_LENGTH(PNAME_OID);
	const CALPONT_OID      *pid_oid   = PID_OID;
	size_t   pid_oid_len = OID_LENGTH(PID_OID);
	const CALPONT_OID      *tid_oid   = TID_OID;
	size_t   tid_oid_len = OID_LENGTH(TID_OID);

	string alarm_data = "CALALARM|" + oam.itoa(alarmID) + "|" + componentID + "|" + oam.itoa(state) + "|" + ModuleName + "|" + processName + "|" + oam.itoa(pid) + "|" + oam.itoa(tid);

	netsnmp_variable_list *notification_vars = NULL;
	
	/*
	* add in the trap definition object 
	*/
	snmp_varlist_add_variable(&notification_vars,
                /*
				* the snmpTrapOID.0 variable 
				*/
				objid_snmptrap, objid_snmptrap_len,
                /*
				* value type is an OID 
				*/
				ASN_OBJECT_ID,
                /*
				* value contents is our notification OID 
				*/
				(u_char *) notification_oid,
                /*
				* size in bytes = oid length * sizeof(oid) 
				*/
				notification_oid_len * sizeof(CALPONT_OID));
	
	/*
    * insert additional objects
	*/
	snmp_varlist_add_variable(&notification_vars,
				calalarm_data_oid, calalarm_data_oid_len,
				ASN_OCTET_STR,
				(u_char*)alarm_data.c_str(), alarm_data.length());
	snmp_varlist_add_variable(&notification_vars,
				component_id_oid, component_id_oid_len,
				ASN_OCTET_STR,
				(u_char*)componentID, strlen((char*)componentID));
			
	snmp_varlist_add_variable(&notification_vars,
				alarm_id_oid, alarm_id_oid_len,
				ASN_INTEGER,
				(u_char*)&alarmID, sizeof(alarmID));
						
	snmp_varlist_add_variable(&notification_vars,
				state_oid, state_oid_len,
				ASN_INTEGER,
				(u_char*)&state, sizeof(state));	

	snmp_varlist_add_variable(&notification_vars,
				sname_oid, sname_oid_len,
				ASN_OCTET_STR,
				(u_char*)ModuleName.c_str(), ModuleName.length());
						
	snmp_varlist_add_variable(&notification_vars,
				pname_oid, pname_oid_len,
				ASN_OCTET_STR,
				(u_char*)processName.c_str(), processName.length());
						
	snmp_varlist_add_variable(&notification_vars,
				pid_oid, pid_oid_len,
				ASN_INTEGER,
				(u_char*)&pid,sizeof(pid_t));

	snmp_varlist_add_variable(&notification_vars,
				tid_oid, tid_oid_len,
				ASN_INTEGER,
				(u_char*)&pid,sizeof(pid_t));

	// NOTE: need to put forward oid ... in snmptrapd.conf	
	send_v2trap(notification_vars); 
	
	//send_trap_vars (6, 17, notification_vars);

//	delete [] alarm_data;
	snmp_free_varbind(notification_vars);
	snmpd_free_trapsinks();
//	free_trap_session((char*)ipAdd.c_str());

	exit (0);
}


// vim:ts=4 sw=4:

