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

/*
* $Id: we_redistribute.h 4450 2013-01-21 14:13:24Z rdempsey $
*/

#ifndef WE_REDISTRIBUTE_H
#define WE_REDISTRIBUTE_H

// forward reference

namespace messageqcpp
{
class ByteStream;
class IOSocket;
}

namespace redistribute
{

class Redistribute
{
  public:
	Redistribute();
	~Redistribute() {};

	static void handleRedistributeMessage(messageqcpp::ByteStream&, messageqcpp::IOSocket&);

  private:
};


} // namespace


#endif  // WE_REDISTRIBUTE_H

// vim:ts=4 sw=4:

