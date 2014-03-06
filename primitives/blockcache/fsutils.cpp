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

#include <unistd.h>
#include <string>
//#define NDEBUG
#include <cassert>
#include <climits>
using namespace std;

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
using namespace boost;
namespace fs=boost::filesystem;

#include "fsutils.h"
#include "exceptclasses.h"

namespace
{

const string resolveInDir(const string& dir, const string& name)
{
	idbassert(!dir.empty() && !name.empty());
	string ret;
	fs::path path(dir);
	if (!fs::exists(path))
		return ret;
	idbassert(fs::exists(path));
	path /= name;
	if (!fs::exists(path))
		return ret;
	idbassert(fs::exists(path));
#ifndef _MSC_VER
	if (!fs::is_symlink(path))
		return ret;
	idbassert(fs::is_symlink(path));
	char* realname = (char*)alloca(PATH_MAX+1);
	ssize_t realnamelen = readlink(path.string().c_str(), realname, PATH_MAX);
	if (realnamelen <= 0)
		return ret;
	realname[realnamelen] = 0;
	fs::path linkname(realname);
	fs::path realpath("/dev");
	realpath /= linkname.filename();
	ret = realpath.string();
#endif
	return ret;
}

inline const string label2dev(const string& name)
{
	return resolveInDir("/dev/disk/by-label", name);
}

inline const string uuid2dev(const string& name)
{
	return resolveInDir("/dev/disk/by-uuid", name);
}

}

namespace fsutils
{

const string symname2devname(const string& sympath)
{
	string ret;
#ifndef _MSC_VER
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep("=");
	tokenizer tokens(sympath, sep);
	tokenizer::iterator tok_iter = tokens.begin();

	idbassert(tok_iter != tokens.end());
	string symtype = *tok_iter;
	if (symtype != "LABEL" && symtype != "UUID")
		return ret;

	idbassert(symtype == "LABEL" || symtype == "UUID");

	++tok_iter;
	idbassert(tok_iter != tokens.end());
	string symname = *tok_iter;

	++tok_iter;
	idbassert(tok_iter == tokens.end());

	if (symtype == "LABEL")
		ret = label2dev(symname);
	else if (symtype == "UUID")
		ret = uuid2dev(symname);
#endif
	return ret;
}

}

