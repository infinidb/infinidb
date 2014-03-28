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

#include <string>
#include "funchelpers.h"
#include "dataconvert.h"

namespace funcexp
{

class TimeExtractor
{
private:
	int32_t m_dayOfWeek;
	int32_t m_dayOfYear;
	int32_t m_weekOfYear;
	bool    m_sundayFirst;

public:
	TimeExtractor() :
		m_dayOfWeek( -1 ),
		m_dayOfYear( -1 ),
		m_weekOfYear( -1 ),
		m_sundayFirst( false ) {;}

	/**
	 * extractTime is an implementation that matches MySQL behavior for the
	 * STR_TO_DATE() function.  See MySQL documentation for details.
	 *
	 * Returns 0 on success, -1 on failure.  On failure the DateTime is
	 * reset to 0s in all fields.
	 */
	int extractTime (const std::string& valStr, const std::string& formatStr, dataconvert::DateTime & dateTime)
	{
		uint32_t fcur = 0;
		uint32_t vcur = 0;
		while( fcur != formatStr.length() )
		{
			if( !handleNextToken(valStr, vcur, formatStr, fcur, dateTime) )
				return returnError( dateTime );
		}

		if( m_dayOfYear > 0 )
		{
			// they set day of year - we also need to make sure there is a year to work with
			if( !dataconvert::isDateValid( 1, 1, dateTime.year ) )
				return returnError( dateTime );

			helpers::get_date_from_mysql_daynr( helpers::calc_mysql_daynr( dateTime.year, 1, 1 ) + m_dayOfYear - 1, dateTime );
		}
		else if( m_weekOfYear > 0 )
		{
			if( m_dayOfWeek < 0 || !dataconvert::isDateValid( 1, 1, dateTime.year ) )
				return returnError( dateTime );

			boost::gregorian::date yearfirst( dateTime.year, 1, 1 );

			// figure out which day of week Jan-01 is
			uint32_t firstweekday = helpers::calc_mysql_weekday( dateTime.year, 1, 1, m_sundayFirst );

			// calculate the offset to the first week starting day
			uint32_t firstoffset = firstweekday ? ( 7 - firstweekday ) : 0;

			firstoffset += ( ( m_weekOfYear - 1) * 7 ) + m_dayOfWeek - ( m_sundayFirst ? 0 : 1 );
			yearfirst += boost::gregorian::date_duration( firstoffset );
			dateTime.year = yearfirst.year();
			dateTime.month = yearfirst.month();
			dateTime.day = yearfirst.day();
		}

		if( !dataconvert::isDateTimeValid( dateTime.hour, dateTime.minute, dateTime.second, dateTime.msecond ) )
			return returnError( dateTime );

		return 0;
	}

private:
	int  returnError( dataconvert::DateTime & dateTime )
	{
		(*(reinterpret_cast<uint64_t *> (&dateTime))) = (uint64_t) 0;
		return -1;
	}

	bool scanDecimalVal(const char *nptr, const char **endptr, int32_t& value)
	{
		value = 0;
		const char* p = nptr;
		while( p < *endptr && isdigit( *p ) )
		{
			value = value * 10 + ((*p) - '0');
			++p;
		}
		*endptr = p;
		return (*endptr != nptr);
	}

	bool handleNextToken(const std::string& valStr, uint32_t& vptr, const std::string& formatStr, uint32_t& fptr, dataconvert::DateTime & dateTime)
	{
		// advance both strings to the first non-whitespace character
		while( valStr[vptr] == ' ' && vptr < valStr.length() )
			++vptr;
		bool vend = (vptr == valStr.length());
		while( formatStr[fptr] == ' ' && fptr < formatStr.length() )
			++fptr;
		bool fend = (fptr == formatStr.length());

		if( vend && fend )
		{
			// apparent trailing whitespace
			return true;
		}
		else if( !vend && !fend )
		{
			if( formatStr[fptr] == '%' )
			{
				// has to be at least one more character in format string
				if( fptr >= formatStr.length() - 1)
					return false;

				fptr++; // skip over %
				// check for special case of %%
				if( formatStr[fptr] == '%' )
				{
					bool ret = formatStr[fptr] == valStr[vptr];
					++fptr;
					++vptr;
					return ret;
				}
				else
				{
					char field = formatStr[fptr];
					++fptr; // also skip the format code
					bool ret = handleField( field, valStr, vptr, dateTime );
					return ret;
				}
			}
			else
			{
				bool ret = formatStr[fptr] == valStr[vptr];
				++fptr;
				++vptr;
				return ret;
			}
		}
		else
		{
			// one string finish before the other one - not good
			return false;
		}
	}

	bool handleField(char field, const std::string& valStr, uint32_t& vptr, dataconvert::DateTime & dateTime)
	{
		int32_t value;
		const char* valptr = valStr.c_str() + vptr;
		switch( field )
		{
			case 'a':
			{
				// weekday abbreviations are always exactly 3 characters
				std::string weekday_str( valStr, vptr, 3 );
				vptr += 3;
				m_dayOfWeek = helpers::convertWeek(weekday_str);
				if( m_dayOfWeek < 0 )
				{
					return false;
				}
				break;
			}
			case 'b':
			{
				// month abbreviations are always exactly 3 characters
				std::string month_str( valStr, vptr, 3 );
				vptr += 3;
				value = helpers::convertMonth(month_str);
				if( value < 0 )
				{
					return false;
				}
				else
				{
					dateTime.month = value;
				}
				break;
			}
			case 'c':
			case 'm':
			{
				// Month, numeric (0..12)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				// we have to do range checking on the month value here because
				// dateTime will arbitrarily truncate to 4 bits and may turn a
				// bad value into a good one
				if( value < 0 || value > 12 )
					return false;
				dateTime.month = value;
				break;
			}
			case 'D':
			case 'd':
			case 'e':
			{
				// %D - Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
				// %d - Day of the month, numeric (00..31)
				// %e - Day of the month, numeric (0..31)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				// now also skip suffix if required - always 2 characters
				if( field == 'D')
					vptr += 2;
				// we have to do range checking on the month value here because
				// dateTime will arbitrarily truncate to 6 bits and may turn a
				// bad value into a good one
				if( value < 0 || value > 31 )
					return false;
				dateTime.day = value;
				break;
			}
			case 'f':
			{
				// 	Microseconds (000000..999999)
				const char* vend = valptr + min(6, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				for( int i = (vend - valptr); i < 6; ++i )
					value = value * 10;
				dateTime.msecond = value;
				break;
			}
			case 'H':
			case 'k':
			{
				// 	Hour (00..23)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				dateTime.hour = value;
				break;
			}
			case 'h':
			case 'I':
			case 'l':
			{
				// 	Hour (01..12)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				dateTime.hour = value;
				break;
			}
			case 'i':
			{
				// 	Minutes, numeric (00..59)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				dateTime.minute = value;
				break;
			}
			case 'j':
			{
				// 	Day of year (001..366)
				const char* vend = valptr + min(3, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				if( value < 1 || value > 366 )
					return false;
				m_dayOfYear = value;
				break;
			}
			case 'M':
			{
				// Month name (January..December)
				// look for the first non-alphabetic character
				size_t endpos = vptr;
				while( tolower(valStr[endpos]) >= 'a' && tolower(valStr[endpos]) <= 'z' && endpos < valStr.length() )
					++endpos;
				std::string month_str( valStr, vptr, endpos );
				vptr += month_str.length();
				value = helpers::convertMonth(month_str);
				if( value < 0 )
				{
					return false;
				}
				else
				{
					dateTime.month = value;
				}
				break;
			}
			case 'p':
			{
				// AM or PM
				if( tolower(valStr[vptr]) == 'p' && tolower(valStr[vptr+1]) == 'm' )
				{
					if( dateTime.hour < 12 )
						dateTime.hour += 12;
				}
				else if( tolower(valStr[vptr]) == 'a' && tolower(valStr[vptr+1]) == 'm' )
				{
					if( dateTime.hour == 12 )
						dateTime.hour = 0;
				}
				else
				{
					vptr += 2;
					return false;
				}
				vptr += 2;
				break;
			}
			case 'r':
			case 'T':
			{
				// Time, 12-hour (hh:mm:ss followed by AM or PM)
				// Time, 24-hour (hh:mm:ss)
				int32_t hour = -1;
				int32_t min  = -1;
				int32_t sec  = -1;
				int32_t numread;
				int32_t sscanf_ck;

				if( ( sscanf_ck = sscanf( valptr, "%2d:%2d:%2d%n", &hour, &min, &sec, &numread) ) != 3 )
				{
					return false;
				}
				valptr += numread;
				vptr += numread;

				dateTime.hour = hour;
				dateTime.minute = min;
				dateTime.second = sec;
				break;
			}
			case 'S':
			case 's':
			{
				// 	Seconds (00..59)
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				dateTime.second = value;
				break;
			}
			case 'U':
			case 'u':
			case 'V':
			case 'v':
			{
				// %U - Week (00..53), where Sunday is the first day of the week
				// %u - Week (00..53), where Monday is the first day of the week
				// %V - Week (01..53), where Sunday is the first day of the week; used with %X
				// %v - Week (01..53), where Monday is the first day of the week; used with %x
				m_sundayFirst = isupper( field );
				const char* vend = valptr + min(2, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				m_weekOfYear = value;
				break;
			}
			case 'X':
			case 'x':
			case 'Y':
			case 'y':
			{
				// %X - Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
				// %x - Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
				// %Y - Year, numeric, four digits
				// %y - Year, numeric (two digits)
				m_sundayFirst = ( field == 'X' );
				int minFieldWidth = ( field == 'y' ? 2 : 4 );
				const char* vend = valptr + min( minFieldWidth, (int)(valStr.length() - vptr) );
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				if( (vend - valptr) <= 2 )
				{
					// regardless of whether field was supposed to be 4 characters.
					// If we read two then apply year 2000 handling
					value += 2000;
					if( value > 2069 )
						value -= 100;
				}
				dateTime.year = value;
				break;
			}
			case 'W':
			{
				// Weekday name (Sunday..Saturday)
				// look for the first non-alphabetic character
				size_t endpos = vptr;
				while( tolower(valStr[endpos]) >= 'a' && tolower(valStr[endpos]) <= 'z' && endpos < valStr.length() )
					++endpos;
				std::string weekday_str( valStr, vptr, endpos );
				vptr += weekday_str.length();
				value = helpers::convertWeek(weekday_str);
				if( value < 0 )
				{
					return false;
				}
				else
				{
					m_dayOfWeek = value;
				}
				break;
			}
			case 'w':
			{
				// Day of the week (0=Sunday..6=Saturday)
				const char* vend = valptr + min(1, (int)(valStr.length() - vptr));
				if( !scanDecimalVal( valptr, &vend, value ) )
					return false;
				vptr += (vend - valptr);
				m_dayOfWeek = value;
				break;
			}
			default:
				return false;
		}
		return true;
	}

};

}
