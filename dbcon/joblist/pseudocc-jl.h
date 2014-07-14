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


#ifndef PSEUDOCCJL_H
#define PSEUDOCCJL_H

#include "columncommand-jl.h"

namespace joblist {

class PseudoCCJL : public ColumnCommandJL
{
	public:
		PseudoCCJL(const PseudoColStep &);
		virtual ~PseudoCCJL();

		virtual void createCommand(messageqcpp::ByteStream &) const;
		virtual void runCommand(messageqcpp::ByteStream &) const;
		virtual std::string toString();
		uint32_t getFunction() const { return function; }
	protected:
	private:
		uint32_t function;
};


}
#endif // PSEUDOCCJL_H
