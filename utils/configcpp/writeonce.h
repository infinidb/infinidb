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
* $Id: writeonce.h 3495 2013-01-21 14:09:51Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef CONFIG_WRITEONCECONFIG_H
#define CONFIG_WRITEONCECONFIG_H

#include <string>
#ifndef _MSC_VER
#include <tr1/unordered_map>
#else
#include <unordered_map>
#endif

#include <stddef.h>

#include "bytestream.h"

namespace config {

/** @brief a write-once config file I/F class
 *
 * This class handles write-once config items
 */
class WriteOnceConfig
{
public:
	/** @brief ctor
	*
	*/
	explicit WriteOnceConfig(const char* cf=0);

	/** @brief ctor
	*
	*/
	explicit WriteOnceConfig(const std::string& cf) :
		fConfigFileName(cf)
	{
		setup();
	}

	/** @brief dtor
	*
	*/
	virtual ~WriteOnceConfig() {}

	/** @brief check if this class owns parm
	*
	*/
	bool owns(const std::string& section, const std::string& name) const {
		return (fEntryMap.find(std::string(section + "." + name)) != fEntryMap.end());
	}

	/** @brief set parm to value
	*
	* If you attempt to set a value more than once, and force is false, this will throw a runtime_error.
	*/
	void setConfig(const std::string& section, const std::string& name, const std::string& value, bool force=false);

	/** @brief get value of parm
	*
	*/
	const std::string getConfig(const std::string& section, const std::string& name) const;

protected:
	/** @brief load from file
	*
	*/
	messageqcpp::ByteStream load();

	/** @brief save to file
	*
	*/
	void save(messageqcpp::ByteStream& ibs) const;

	/** @brief serialize to ByteStream
	*
	*/
	virtual void serialize(messageqcpp::ByteStream& obs) const;

	/** @brief load from ByteStream
	*
	*/
	virtual void unserialize(messageqcpp::ByteStream& ibs);

private:
	typedef std::pair<std::string, bool> ConfigItem_t;
	typedef std::tr1::unordered_map<std::string, ConfigItem_t*> EntryMap_t;

	static const uint32_t WriteOnceConfigVersion = 1;

	//defaults okay
	//WriteOnceConfig(const WriteOnceConfig& rhs);
	//WriteOnceConfig& operator=(const WriteOnceConfig& rhs);

	/** @brief ctor helper
	*
	*/
	void setup();

	/** @brief setup defaults when file doesn't exist
	*
	*/
	void initializeDefaults();

	EntryMap_t fEntryMap;

	std::string fConfigFileName;

	ConfigItem_t fLBID_Shift;
	ConfigItem_t fDBRootCount;
	ConfigItem_t fDBRMRoot;
	ConfigItem_t fSharedMemoryTmpFile1;
	ConfigItem_t fTxnIDFile;
	ConfigItem_t fSharedMemoryTmpFile2;

};

}

#endif

