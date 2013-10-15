//------------------------------------------------------------------------------
// test driver used to test reloadable cache enhancement to we_config
// for bug 4486
// $Id$
//------------------------------------------------------------------------------
#include <iostream>
#include <vector>
#include <dbrm.h>

#include "we_config.h"

using namespace WriteEngine;

void test()
{
    std::cout << "getDBRootByIdx(1):           " <<
        Config::getDBRootByIdx(1) << std::endl;
    std::cout << "getDBRootByIdx(3):           " <<
        Config::getDBRootByIdx(3) << std::endl;

    std::cout << "getDBRootByNum(1):           " <<
        Config::getDBRootByNum(1) << std::endl;
    std::cout << "getDBRootByNum(3):           " <<
        Config::getDBRootByNum(3) << std::endl;

    std::vector<unsigned short> dbRootIds;
    Config::getRootIdList( dbRootIds );
    std::cout << "getRootIdList:               ";
    for (unsigned k=0; k<dbRootIds.size(); k++) {
        std::cout << dbRootIds[k] << ' '; }
    std::cout << std::endl;

    std::vector<std::string> dbRootPathList;
    Config::getDBRootPathList( dbRootPathList );
    std::cout << "getDBRootPathList: " << std::endl;
    for (unsigned k=0; k<dbRootPathList.size(); k++)
        std::cout << "  " << k << ". " << dbRootPathList[k] << std::endl;

    std::cout << "getBulkRoot():               " <<
        Config::getBulkRoot() << std::endl;
    std::cout << "DBRootCount():               " <<
        Config::DBRootCount() << std::endl;
    std::cout << "totalDBRootCount():          " <<
        Config::totalDBRootCount() << std::endl;
    std::cout << "getWaitPeriod():             " <<
        Config::getWaitPeriod() << std::endl;
    std::cout << "getFilePerColumnPartition(): " <<
        Config::getFilesPerColumnPartition() << std::endl;
    std::cout << "getExtentsPerSegmentFile():  " <<
        Config::getExtentsPerSegmentFile() << std::endl;
    std::cout << "getBulkProcessPriority():    " <<
        Config::getBulkProcessPriority() << std::endl;
    std::cout << "getBulkRollbackDir():        " <<
        Config::getBulkRollbackDir() << std::endl;
    std::cout << "getMaxFileSystemDiskUsage(): " <<
        Config::getMaxFileSystemDiskUsage() << std::endl;
    std::cout << "getNumCompressedPadBlks():   " <<
        Config::getNumCompressedPadBlks() << std::endl;
    std::cout << "getParentOAMModuleFlag():    " <<
        Config::getParentOAMModuleFlag() << std::endl;
    std::cout << "getLocalModuleType():        " <<
        Config::getLocalModuleType() << std::endl;
    std::cout << "getLocalModuleID():          " <<
        Config::getLocalModuleID() << std::endl;
    std::cout << "getVBRoot():                 " <<
        Config::getVBRoot() << std::endl;
}

int main()
{
    Config::initConfigCache();
    char resp;

    int nTest = 0;
    std::cout << std::endl;
    while (1)
    {
        std::cout << "test" << nTest << "..." << std::endl;
        test();
        std::cout << "Pause..." << std::endl;
        std::cin  >> resp;
        std::cout << std::endl;
        if (resp == 'c')
        {
            std::cout << "Has local DBRootList changed: " <<
                (bool)Config::hasLocalDBRootListChanged() << std::endl;
        }
        else if (resp == 'q')
        {
            break;
        }
        nTest++;
    }

    return 0;
}
