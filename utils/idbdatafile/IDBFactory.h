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

#ifndef IDBFACTORY_H_
#define IDBFACTORY_H_

#include <string>
#include <map>
#include <stdexcept>

#include "IDBDataFile.h"

namespace idbdatafile
{

class FileFactoryBase;
class IDBFileSystem;

struct FileFactoryEnt
{
	FileFactoryEnt() :
		type(IDBDataFile::UNKNOWN),
		name("unknown"),
		factory(0),
		filesystem(0) {;}

	FileFactoryEnt(
			IDBDataFile::Types t,
			const std::string& n,
			FileFactoryBase* f,
			IDBFileSystem* fs) :
		type(t),
		name(n),
		factory(f),
		filesystem(fs) {;}

	IDBDataFile::Types     type;
	std::string	           name;
	FileFactoryBase*       factory;
	IDBFileSystem*         filesystem;
};

typedef FileFactoryEnt (*FileFactoryEntryFunc)();

/**
 * IDBFactory manages the factory plugins that know how to create
 * files of different types.  The plugin architecture allows IDB
 * to defer dependencies until plugin load.  This class is a static
 * class and is threadsafe.
 *
 */
class IDBFactory
{
public:
	/**
	 * This method installs the default plugs for Buffered and Unbuffered files.
	 * It is called automatically from the IDBPolicy::init() body so that clients
	 * don't have to worry about it.
	 */
	static bool installDefaultPlugins();

	/**
	 * This method installs a dynamic plugin.  The plugin argument must refer
	 * to a ".so" file that exposes an extern "C" functions:
	 *     FileFactoryEnt   plugin_instance()
	 */
	static bool installPlugin(const std::string& plugin);

	/**
	 * This method calls the Factory for the specified type
	 */
	static IDBDataFile* open(IDBDataFile::Types type, const char* fname, const char* mode, unsigned opts, unsigned colWidth);

	/**
	 * This retrieves the IDBFileSystem for the specified type
	 */
	static IDBFileSystem& getFs(IDBDataFile::Types type);

	/**
	 * This retrieves the IDBFileSystem for the specified type
	 */
	static const std::string& name(IDBDataFile::Types type);

private:
	typedef std::map<IDBDataFile::Types,FileFactoryEnt> FactoryMap;
	typedef FactoryMap::const_iterator FactoryMapCIter;

	static FactoryMap s_plugins;

	IDBFactory();
	virtual ~IDBFactory();
};

inline
const std::string& IDBFactory::name(IDBDataFile::Types type)
{
	if( s_plugins.find(type) == s_plugins.end() )
	{
    	throw std::runtime_error("unknown plugin type in IDBFactory::name");
	}

	return s_plugins[type].name;
}

}

#endif /* IDBFACTORY_H_ */
