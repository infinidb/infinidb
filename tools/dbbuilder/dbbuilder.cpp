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
* $Id: dbbuilder.cpp 1673 2012-01-26 17:21:50Z pleblanc $
*
*******************************************************************************/
#include <stdio.h>
#include <unistd.h>
#include <cerrno>
#include <stdexcept>
using namespace std;

#include "dbbuilder.h"
#include "systemcatalog.h"
#include "tpchschema.h"
#include "tpchpopulate.h"
#include "liboamcpp.h"
using namespace oam;
using namespace dmlpackageprocessor;
using namespace dmlpackage;

#include "objectidmanager.h"
using namespace execplan;

enum BUILD_OPTION
{
    PART_ONLY = 0,  // only insert 5 rows to part table
    SMALL_SIZE = 1, // 1000 rows in lineitem
    LARGE_SIZE = 2,  // 0.1G full database
    SCHEMA_ONLY = 3,  // only TPCH schema, no index
    INDEX_ONLY = 4,  // only index
    SCHEMA_INDEX = 5, //only sigle column index
    SCHEMA_MULTICOL_INDEX = 6, // schema with multicolumn index
    SYSCATALOG_ONLY = 7, //Create systables only
    USER_SCHEMA = 8, //take user input schema, append to table creation
};

namespace {

int setUp()
{
#ifndef _MSC_VER
  	(void)system("/bin/rm -f /tmp/dbbuilder.status >/dev/null 2>&1");
  	(void)system("/bin/touch /tmp/dbbuilder.status >/dev/null 2>&1");
#endif
	return 0;
}

int checkNotThere(WriteEngine::FID fid)
{
	WriteEngine::FileOp fileOp;

	return (fileOp.existsOIDDir(fid) ? -1 : 0);
}

void tearDown()
{
    unlink("/tmp/oidbitmap");
}

void usage()
{
    cerr << "Usage: dbbuilder [-h|f] [-u schemaname] function" << endl
         << "  -h Display this help info" << endl
         << "  -f Necessary to use any fcn other than 7" << endl
         << " fcn" << endl
         << "  0  Build all TPCH tables. Only insert 5 rows to PART" << endl
         << "  1  Build and insert all TPCH tables. LINEITEM has 1000 rows" << endl
         << "  2  Build larger TPCH database, need to give data size later" << endl
         << "  3  Build all TPCH tables without index. Don't insert any data" << endl
         << "  4  Create index on TPCH tables only " << endl
         << "  5  Build all TPCH tables with index. Don't insert any data" << endl
         << "  6  Build all TPCH tables with multi column index. Don't insert any data" << endl
         << "  7  Build system tables only" << endl
         << "  8  Build all TPCH tables with user input schema. Use -u schemaname to input schema" << endl
         << endl
         << "WARNING! Using this tool improperly can render your database unusable!" << endl
		;
}

const unsigned sysCatalogErr = logging::M0060;

void errorHandler(const unsigned mid,
	const string& src,
	const string& msg,
	bool  isCritErr=true)
{
      logging::LoggingID lid(19);
      logging::MessageLog ml(lid);
      logging::Message::Args args;
      logging::Message message(mid);
      if (isCritErr)
      {
          args.add(string("error"));
          args.add(msg);
          message.format( args );
          ml.logCriticalMessage(message );
          cout <<  src << " was not successful. " << msg << endl;
      }
      else
      {
          args.add(string("status"));
          args.add(msg);
          message.format( args );
          ml.logInfoMessage(message );
          cout <<  src << " was not completed. " << msg << endl;
      }
}


}

int main(int argc, char* argv[])
{
	int buildOption;
	int c;
	std::string schema("tpch");
	Oam oam;
	bool fFlg = false;

	opterr = 0;

	while ((c = getopt(argc, argv, "u:fh")) != EOF)
		switch (c)
		{
		case 'u':
			schema = optarg;
			break;
		case 'f':
			fFlg = true;
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) < 1)
	{
		usage();
		return 1;
	}

	oamModuleInfo_t t;
	bool parentOAMModuleFlag = false;

	//get local module info; validate running on Active Parent OAM Module
	try {
		t = oam.getModuleInfo();
		parentOAMModuleFlag = boost::get<4>(t);
	}
	catch (exception&) {
		parentOAMModuleFlag = true;
	}

	if ( !parentOAMModuleFlag )
	{
		cerr << "Exiting, dbbuilder can only be run on the Active "
			"Parent OAM Module" << endl;
		return 1;
	}

	buildOption = atoi(argv[optind++]);

	if (buildOption != 7 && !fFlg)
	{
		usage();
		return 1;
	}

	if (buildOption == LARGE_SIZE)
	{
		(void)system("./dataGen.pl");
	}

#if 0
	if ( buildOption == INDEX_ONLY	)
	{
		(void)system("rm Job_300.xml >/dev/null 2>&1");
		TpchSchema db;
		db.buildIndex();
		return 0;
	}
	else if ( buildOption == SCHEMA_MULTICOL_INDEX )
	{
		try
		{
			setUp();
			SystemCatalog sysCatalog;
			sysCatalog.build();
			TpchSchema db;

			db.buildMultiColumnIndex();
		}
		catch (exception& ex)
		{
			cerr << ex.what() << endl;
		}
		catch (...)
		{
			cerr << "Caught unknown exception!" << endl;
		}
	}
	else
#endif
       if ( buildOption == SYSCATALOG_ONLY )
	{
		setUp();

		bool canWrite = true;
		if (access("/tmp/dbbuilder.status", W_OK) != 0)
			canWrite = false;

		try
		{
			if (checkNotThere(1001) != 0)
			{
				string cmd = "echo 'FAILED: buildOption=" +
					oam.itoa(buildOption) +
					"' > /tmp/dbbuilder.status";

				if (canWrite)
					(void)system(cmd.c_str());
				else
					cerr << cmd << endl;
				errorHandler(sysCatalogErr,
					"Build system catalog",
					"System catalog appears to exist.  It will remain intact "
						"for reuse.  The database is not recreated.",
						false);
				return 1;
			}

			//create an initial oid bitmap file
			{
				ObjectIDManager oidm;
			}

			SystemCatalog sysCatalog;
			sysCatalog.build();

			std::string cmd = "echo 'OK: buildOption=" + oam.itoa(buildOption) + "' > /tmp/dbbuilder.status";

			if (canWrite)
				(void)system(cmd.c_str());
			else
#ifdef _MSC_VER
				(void)0;
#else
				cerr << cmd << endl;
#endif

			cmd = "/usr/local/Calpont/bin/save_brm";
			if (canWrite) {
				int err;

				err = system(cmd.c_str());
				if (err != 0) {
					ostringstream os;
					os << "Warning: running " << cmd << " failed.  This is usually non-fatal.";
					cerr << os << endl;
					errorHandler(sysCatalogErr, "Save BRM", os.str());
				}
			}
			else
				cerr << cmd << endl;

			return 0;
		}
		catch (exception& ex)
		{
			string cmd = "echo 'FAILED: buildOption=" + oam.itoa(buildOption) + "' > /tmp/dbbuilder.status";

			if (canWrite)
				(void)system(cmd.c_str());
			else
				cerr << cmd << endl;
			errorHandler(sysCatalogErr, "Build system catalog", ex.what());
		}
		catch (...)
		{
			string cmd = "echo 'FAILED: buildOption=" + oam.itoa(buildOption) + "' > /tmp/dbbuilder.status";

			if (canWrite)
				(void)system(cmd.c_str());
			else
				cerr << cmd << endl;
			string err("Caught unknown exception!");
			errorHandler(sysCatalogErr, "Build system catalog", err);
		}
	}
	else if ( buildOption == USER_SCHEMA )
	{
		setUp();
		try
		{
			TpchSchema db;

			db.buildTpchTables( schema);
			return 0;
		}

		catch (exception& ex)
		{
			cerr << ex.what() << endl;
		}
		catch (...)
		{
			cerr << "Caught unknown exception!" << endl;
		}
	}
	else
	{
		try
		{
			setUp();
			SystemCatalog sysCatalog;
			sysCatalog.build();

			TpchSchema db;

			db.build();

			TpchPopulate pop;

			if (buildOption == PART_ONLY)
				pop.populate_part();

			else if (buildOption == LARGE_SIZE)
			{
				pop.populate_tpch();
			}

			else if (buildOption == SMALL_SIZE)
			{          
				std::string fileName = "srcdata/parttable.csv";
				pop.populateFromFile( "tpch.part", fileName );

				fileName = "srcdata/customertable.csv";        
				pop.populateFromFile( "tpch.customer", fileName );

				fileName = "srcdata/lineitemtable.csv";
				pop.populateFromFile( "tpch.lineitem", fileName );

				fileName = "srcdata/nationtable.csv";
				pop.populateFromFile( "tpch.nation", fileName );

				fileName = "srcdata/ordertable.csv";
				pop.populateFromFile( "tpch.orders", fileName );

				fileName = "srcdata/partsupptable.csv";
				pop.populateFromFile( "tpch.partsupp", fileName );

				fileName = "srcdata/regiontable.csv";
				pop.populateFromFile( "tpch.region", fileName );

				fileName = "srcdata/suppliertable.csv";
				pop.populateFromFile( "tpch.supplier", fileName );
			}

			else if (buildOption == SCHEMA_INDEX || buildOption == INDEX_ONLY || buildOption == SCHEMA_ONLY )
			{
				return 0;
			}

			else
			{
				cerr << "Invalid option" << endl;
				usage();
				return 0;
			}

			//Commit
			std::string command("COMMIT;");
			VendorDMLStatement dml_command(command, 1);
			CalpontDMLPackage* dmlCommandPkgPtr = CalpontDMLFactory::makeCalpontDMLPackage(dml_command);

			DMLPackageProcessor* pkgProcPtr =
			DMLPackageProcessorFactory::makePackageProcessor(DML_COMMAND, *dmlCommandPkgPtr );

			DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *dmlCommandPkgPtr );

			if ( DMLPackageProcessor::NO_ERROR != result.result )
			{
				cout << "Commit failed!" << endl;
			}
			delete pkgProcPtr;
			delete dmlCommandPkgPtr;   

			tearDown();

			return 0;

		}
		catch (exception& ex)
		{
			cerr << ex.what() << endl;
		}
		catch (...)
		{
			cerr << "Caught unknown exception!" << endl;
		}
	}
	return 1;
}
// vim:ts=4 sw=4:

