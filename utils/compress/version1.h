/******************************************************************************************
* $Id$
*
******************************************************************************************/
#ifndef COMP_VERSION1_H__
#define COMP_VERSION1_H__

#include <cstddef>
#include <stdint.h>

namespace compress
{
	namespace v1
	{

		bool decompress(const char* in, const uint32_t inLen, unsigned char* out, size_t* ol);

	} //namespace v1
} // namespace compress

#endif

