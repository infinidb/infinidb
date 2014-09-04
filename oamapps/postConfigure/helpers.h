#ifndef HELPERS_H__
#define HELPERS_H__

#include "liboamcpp.h"

using namespace messageqcpp;
namespace installer
{
extern bool waitForActive();
extern void dbrmDirCheck();
extern void mysqlSetup();
extern int sendMsgProcMon( std::string module, ByteStream msg, int requestID, int timeout );
extern int sendUpgradeRequest(int IserverTypeInstall);
extern void checkFilesPerPartion(int DBRootCount, Config* sysConfig);
}

#endif

