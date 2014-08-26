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

/****************************************************************************
* $Id: dataconvert.h 3693 2013-04-05 16:11:30Z chao $
*
*
****************************************************************************/
/** @file */

#ifndef DATACONVERT_H
#define DATACONVERT_H

#include <unistd.h>
#include <string>
#include <boost/any.hpp>
#include <vector>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif
#include <boost/regex.hpp>

#include "calpontsystemcatalog.h"
#include "columnresult.h"
#include "exceptclasses.h"

// remove this block if the htonll is defined in library
#ifdef __linux__
#include <endian.h>
#if __BYTE_ORDER == __BIG_ENDIAN       // 4312
inline uint64_t htonll(uint64_t n)
{ return n; }
#elif __BYTE_ORDER == __LITTLE_ENDIAN  // 1234
inline uint64_t htonll(uint64_t n)
{
return ((((uint64_t) htonl(n & 0xFFFFFFFFLLU)) << 32) | (htonl((n & 0xFFFFFFFF00000000LLU) >> 32)));
}
#else  // __BYTE_ORDER == __PDP_ENDIAN    3412
inline uint64_t htonll(uint64_t n);
// don't know 34127856 or 78563412, hope never be required to support this byte order.
#endif
#else //!__linux__
#if _MSC_VER < 1600
//Assume we're on little-endian
inline uint64_t htonll(uint64_t n)
{
return ((((uint64_t) htonl(n & 0xFFFFFFFFULL)) << 32) | (htonl((n & 0xFFFFFFFF00000000ULL) >> 32)));
}
#endif //_MSC_VER
#endif //__linux__

// this method evalutes the uint64 that stores a char[] to expected value
inline uint64_t uint64ToStr(uint64_t n)
{ return htonll(n); }


#if defined(_MSC_VER) && defined(xxxDATACONVERT_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

const int64_t IDB_pow[19] = {
1,
10,
100,
1000,
10000,
100000,
1000000,
10000000,
100000000,
1000000000,
10000000000LL,
100000000000LL,
1000000000000LL,
10000000000000LL,
100000000000000LL,
1000000000000000LL,
10000000000000000LL,
100000000000000000LL,
1000000000000000000LL
};


namespace dataconvert
{

enum CalpontDateTimeFormat
{
    CALPONTDATE_ENUM     = 1, // date format is: "YYYY-MM-DD"
    CALPONTDATETIME_ENUM = 2  // date format is: "YYYY-MM-DD HH:MI:SS"
};


/** @brief a structure to hold a date
 */
struct Date
{
    unsigned spare  : 6;
    unsigned day    : 6;
    unsigned month  : 4;
    unsigned year   : 16;
    // NULL column value = 0xFFFFFFFE
    Date( ) :
        spare(0x3E), day(0x3F), month(0xF), year(0xFFFF) {}
    // Construct a Date from a 64 bit integer Calpont date.
    Date(uint64_t val) :
        spare(0x3E), day((val >> 6) & 077), month((val >> 12) & 0xF), year((val >> 16)) {}
    // Construct using passed in parameters, no value checking
    Date(unsigned y, unsigned m, unsigned d) : spare(0x3E), day(d), month(m), year(y) {}

    int32_t convertToMySQLint() const;
};

inline
int32_t Date::convertToMySQLint() const
{
    return (int32_t) (year*10000)+(month*100)+day;
}

/** @brief a structure to hold a datetime
 */
struct DateTime
{
    unsigned msecond : 20;
    unsigned second  : 6;
    unsigned minute  : 6;
    unsigned hour    : 6;
    unsigned day     : 6;
    unsigned month   : 4;
    unsigned year    : 16;
    // NULL column value = 0xFFFFFFFFFFFFFFFE
    DateTime( ) :
        msecond(0xFFFFE), second(0x3F), minute(0x3F), hour(0x3F), day(0x3F), month(0xF), year(0xFFFF) {}
    // Construct a DateTime from a 64 bit integer Calpont datetime.
    DateTime(uint64_t val) :
        msecond(val & 0xFFFFF), second((val >> 20) & 077), minute((val >> 26) & 077),
        hour((val >> 32) & 077), day((val >> 38) & 077), month((val >> 44) & 0xF),
        year(val >> 48) {}
    // Construct using passed in parameters, no value checking
    DateTime(unsigned y, unsigned m, unsigned d, unsigned h, unsigned min, unsigned sec, unsigned msec) :
        msecond(msec), second(sec), minute(min), hour(h), day(d), month(m), year(y) {}

    int64_t convertToMySQLint() const;
    void    reset();
};

inline
int64_t DateTime::convertToMySQLint() const
{
    return (int64_t) (year*10000000000LL)+(month*100000000)+(day*1000000)+(hour*10000)+(minute*100)+second;
}

inline
void    DateTime::reset()
{
    msecond = 0xFFFFE;
    second  = 0x3F;
    minute  = 0x3F;
    hour    = 0x3F;
    day     = 0x3F;
    month   = 0xF;
    year    = 0xFFFF;
}

/** @brief a structure to hold a time
 *  range: -838:59:59 ~ 838:59:59
 */
struct Time
{
    signed msecond : 24;
    signed second  : 8;
    signed minute  : 8;
    signed hour    : 12;
    signed day     : 12;
    
    // NULL column value = 0xFFFFFFFFFFFFFFFE
    Time() : msecond (0xFFFFFE),
             second (0xFF),
             minute (0xFF),
             hour (0xFFF),
             day (0xFFF){}

    // Construct a Time from a 64 bit integer InfiniDB time.
    Time(int64_t val) :
        msecond(val & 0xffffff),
        second((val >> 24) & 0xff),
        minute((val >> 32) & 0xff),
        hour((val >> 40) & 0xfff),
        day((val >> 52) & 0xfff)
        {}
};

static uint32_t daysInMonth[13] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

inline uint32_t getDaysInMonth(uint32_t month)
{ return ( (month < 1 || month > 12) ? 0 : daysInMonth[month-1]);}

inline bool isLeapYear ( int year)
{
    if( year % 400 == 0 )
        return true;
    if( ( year % 4 == 0 ) && ( year % 100 != 0 ) )
        return true;
    return false;
}

inline
bool isDateValid ( int day, int month, int year)
{
    bool valid = true;
    int daycheck = getDaysInMonth( month );
    if( month == 2 && isLeapYear( year ) )
        //  29 days in February in a leap year
        daycheck = 29;
    if ( ( year < 1400 ) || ( year > 9999 ) )
        valid = false;
    else if ( month < 1 || month > 12 )
        valid = false;
    else if ( day < 1 || day > daycheck )
        valid = false;
    return ( valid );
}

inline
bool isDateTimeValid ( int hour, int minute, int second, int microSecond)
{
    bool valid = false;
    if ( hour >= 0 && hour <= 24 )
    {
        if ( minute >= 0 && minute < 60 )
        {
            if ( second >= 0 && second < 60 )
            {
                if ( microSecond >= 0 && microSecond <= 999999 )
                {
                    valid = true;
                }
            }
        }
    }
    return valid;
}

inline
int64_t string_to_ll( const std::string& data, bool& bSaturate )
{
    // This function doesn't take into consideration our special values
    // for NULL and EMPTY when setting the saturation point. Should it?
    char *ep = NULL;
    const char *str = data.c_str();
    errno = 0;
    int64_t value = strtoll(str, &ep, 10);

    //  (no digits) || (more chars)  || (other errors & value = 0)
    if ((ep == str) || (*ep != '\0') || (errno != 0 && value == 0))
        throw logging::QueryDataExcept("value is not numerical.", logging::formatErr);

    if (errno == ERANGE && (value == std::numeric_limits<int64_t>::max() || value == std::numeric_limits<int64_t>::min()))
        bSaturate = true;

    return value;
}

inline
uint64_t string_to_ull( const std::string& data, bool& bSaturate )
{
    // This function doesn't take into consideration our special values
    // for NULL and EMPTY when setting the saturation point. Should it?
    char *ep = NULL;
    const char *str = data.c_str();
    errno = 0;

    // check for negative number. saturate to 0;
    if (data.find('-') != data.npos)
    {
        bSaturate = true;
        return 0;
    }
    uint64_t value = strtoull(str, &ep, 10);
    //  (no digits) || (more chars)  || (other errors & value = 0)
    if ((ep == str) || (*ep != '\0') || (errno != 0 && value == 0))
        throw logging::QueryDataExcept("value is not numerical.", logging::formatErr);

    if (errno == ERANGE && (value == std::numeric_limits<uint64_t>::max()))
        bSaturate = true;

    return value;
}

/** @brief DataConvert is a component for converting string data to Calpont format
  */
class DataConvert
{
public:

    /**
     * @brief convert a columns data, represnted as a string, to it's native
     * format
     *
     * @param type the columns data type
     * @param data the columns string representation of it's data
     */
    EXPORT static boost::any convertColumnData( const execplan::CalpontSystemCatalog::ColType& colType,
                                                  const std::string& dataOrig, bool& bSaturate,
                                                bool nulFlag = false, bool noRoundup = false, bool isUpdate = false);

   /**
     * @brief convert a columns data from native format to a string
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    EXPORT static std::string dateToString( int  datevalue );  
    static inline void dateToString( int datevalue, char* buf, unsigned int buflen );

   /**
     * @brief convert a columns data from native format to a string
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    EXPORT static std::string datetimeToString( long long  datetimevalue );      
    static inline void datetimeToString( long long datetimevalue, char* buf, unsigned int buflen );

   /**
     * @brief convert a columns data from native format to a string
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    EXPORT static std::string dateToString1( int  datevalue );  
    static inline void dateToString1( int datevalue, char* buf, unsigned int buflen );

   /**
     * @brief convert a columns data from native format to a string
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    EXPORT static std::string datetimeToString1( long long  datetimevalue );      
    static inline void datetimeToString1( long long datetimevalue, char* buf, unsigned int buflen );

    /**
     * @brief convert a date column data, represnted as a string, to it's native
     * format. This function is for bulkload to use.
     *
     * @param type the columns data type
     * @param dataOrig the columns string representation of it's data
     * @param dateFormat the format the date value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     */
    EXPORT static int32_t convertColumnDate( const char* dataOrg,
                                  CalpontDateTimeFormat dateFormat,
                                  int& status, unsigned int dataOrgLen );

    /**
     * @brief Is specified date valid; used by binary bulk load
     */
    EXPORT static bool      isColumnDateValid( int32_t date );
                                                                 
    /**
     * @brief convert a datetime column data, represented as a string,
     * to it's native format. This function is for bulkload to use.
     *
     * @param type the columns data type
     * @param dataOrig the columns string representation of it's data
     * @param datetimeFormat the format the date value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     */
    EXPORT static int64_t convertColumnDatetime( const char* dataOrg,
                                  CalpontDateTimeFormat datetimeFormat,
                                  int& status, unsigned int dataOrgLen );  

    /**
     * @brief Is specified datetime valid; used by binary bulk load
     */
    EXPORT static bool      isColumnDateTimeValid( int64_t dateTime );

    EXPORT static bool isNullData(execplan::ColumnResult* cr, int rownum, execplan::CalpontSystemCatalog::ColType colType);
    static inline std::string decimalToString(int64_t value, uint8_t scale, execplan::CalpontSystemCatalog::ColDataType colDataType);
    static inline void decimalToString(int64_t value, uint8_t scale, char* buf, unsigned int buflen, execplan::CalpontSystemCatalog::ColDataType colDataType);
    static inline std::string constructRegexp(const std::string& str);
    static inline bool isEscapedChar(char c) { return ('%' == c || '_' == c); }
    
    // convert string to date
    EXPORT static int64_t stringToDate(const std::string& data);
    // convert string to datetime
    EXPORT static int64_t stringToDatetime(const std::string& data, bool* isDate = NULL);
    // convert integer to date
    EXPORT static int64_t intToDate(int64_t data);
    // convert integer to datetime
    EXPORT static int64_t intToDatetime(int64_t data, bool* isDate = NULL);
    
    // convert string to date. alias to stringToDate
    EXPORT static int64_t dateToInt(const std::string& date);
    // convert string to datetime. alias to datetimeToInt
    EXPORT static int64_t datetimeToInt(const std::string& datetime);
    EXPORT static int64_t stringToTime (const std::string& data);
    // bug4388, union type conversion
    EXPORT static execplan::CalpontSystemCatalog::ColType convertUnionColType(std::vector<execplan::CalpontSystemCatalog::ColType>&);
};

inline void DataConvert::dateToString( int datevalue, char* buf, unsigned int buflen)
{
    snprintf( buf, buflen, "%04d-%02d-%02d",
                (unsigned)((datevalue >> 16) & 0xffff),
                (unsigned)((datevalue >> 12) & 0xf),
                (unsigned)((datevalue >> 6) & 0x3f)
            );
}

inline void DataConvert::datetimeToString( long long datetimevalue, char* buf, unsigned int buflen )
{
    snprintf( buf, buflen, "%04d-%02d-%02d %02d:%02d:%02d", 
                    (unsigned)((datetimevalue >> 48) & 0xffff), 
                    (unsigned)((datetimevalue >> 44) & 0xf),
                    (unsigned)((datetimevalue >> 38) & 0x3f),
                    (unsigned)((datetimevalue >> 32) & 0x3f),
                    (unsigned)((datetimevalue >> 26) & 0x3f),
                    (unsigned)((datetimevalue >> 20) & 0x3f)
                );
}

inline void DataConvert::dateToString1( int datevalue, char* buf, unsigned int buflen)
{
    snprintf( buf, buflen, "%04d%02d%02d",
                (unsigned)((datevalue >> 16) & 0xffff),
                (unsigned)((datevalue >> 12) & 0xf),
                (unsigned)((datevalue >> 6) & 0x3f)
            );
}

inline void DataConvert::datetimeToString1( long long datetimevalue, char* buf, unsigned int buflen )
{
    snprintf( buf, buflen, "%04d%02d%02d%02d%02d%02d", 
                    (unsigned)((datetimevalue >> 48) & 0xffff), 
                    (unsigned)((datetimevalue >> 44) & 0xf),
                    (unsigned)((datetimevalue >> 38) & 0x3f),
                    (unsigned)((datetimevalue >> 32) & 0x3f),
                    (unsigned)((datetimevalue >> 26) & 0x3f),
                    (unsigned)((datetimevalue >> 20) & 0x3f)
                );
}

inline std::string DataConvert::decimalToString(int64_t value, uint8_t scale, execplan::CalpontSystemCatalog::ColDataType colDataType)
{
    char buf[80];
    DataConvert::decimalToString(value, scale, buf, 80, colDataType);
    return std::string(buf);
}

inline void DataConvert::decimalToString(int64_t int_val, uint8_t scale, char* buf, unsigned int buflen,
                                         execplan::CalpontSystemCatalog::ColDataType colDataType)
{
    // Need to convert a string with a binary unsigned number in it to a 64-bit signed int
    
    // MySQL seems to round off values unless we use the string store method. Groan.
    // Taken from ha_calpont_impl.cpp
    
    //biggest Calpont supports is DECIMAL(18,x), or 18 total digits+dp+sign for column
    // Need 19 digits maxium to hold a sum result of 18 digits decimal column.
    if (isUnsigned(colDataType))
    {
#ifndef __LP64__
        snprintf(buf, buflen, "%llu", static_cast<uint64_t>(int_val));
#else
        snprintf(buf, buflen, "%lu", static_cast<uint64_t>(int_val));
#endif
    }
    else
    {
#ifndef __LP64__
        snprintf(buf, buflen, "%lld", int_val);
#else
        snprintf(buf, buflen, "%ld", int_val);
#endif
    }

    if (scale == 0)
        return;

    //we want to move the last dt_scale chars right by one spot to insert the dp
    //we want to move the trailing null as well, so it's really dt_scale+1 chars
    size_t l1 = strlen(buf);
    char* ptr = &buf[0];
    if (int_val < 0)
    {
        ptr++;
        idbassert(l1 >= 2);
        l1--;
    }
    //need to make sure we have enough leading zeros for this to work...
    //at this point scale is always > 0
    size_t l2 = 1;
    if ((unsigned)scale > l1)
    {
        const char* zeros = "00000000000000000000"; //20 0's
        size_t diff=0;
        if (int_val != 0)
            diff = scale - l1; //this will always be > 0
        else
            diff = scale;
        memmove((ptr + diff), ptr, l1 + 1); //also move null
        memcpy(ptr, zeros, diff);
        if (int_val != 0)
            l1 = 0;
        else
            l1 = 1;
    }
    else if ((unsigned)scale == l1)
    {
        l1 = 0;
        l2 = 2;
    }
    else
    {
        l1 -= scale;
    }
    memmove((ptr + l1 + l2), (ptr + l1), scale + 1); //also move null

    if (l2 == 2)
        *(ptr + l1++) = '0';

    *(ptr + l1) = '.';
}


//FIXME: copy/pasted from dictionary.cpp: refactor
inline std::string DataConvert::constructRegexp(const std::string& str)
{
    //In the worst case, every char is quadrupled, plus some leading/trailing cruft...
    char* cBuf = (char*)alloca(((4 * str.length()) + 3) * sizeof(char));
    char c;
    uint32_t i, cBufIdx = 0;
    // translate to regexp symbols
    cBuf[cBufIdx++] = '^';  // implicit leading anchor
    for (i = 0; i < str.length(); i++) {
        c = (char) str.c_str()[i];
        switch (c) {

            // chars to substitute
            case '%':
                cBuf[cBufIdx++] = '.';
                cBuf[cBufIdx++] = '*';
                break;
            case '_':
                cBuf[cBufIdx++] = '.';
                break;

            // escape the chars that are special in regexp's but not in SQL
            // default special characters in perl: .[{}()\*+?|^$
            case '.':
            case '*':
            case '^':
            case '$':
             case '?':
             case '+':
             case '|':
             case '[':
             case '{':
             case '}':
             case '(':
             case ')':
                cBuf[cBufIdx++] = '\\';
                cBuf[cBufIdx++] = c;
                break;
            case '\\':  //this is the sql escape char
                if ( i + 1 < str.length())
                {
                    if (isEscapedChar(str.c_str()[i+1]))
                    {
                        cBuf[cBufIdx++] = str.c_str()[++i];
                        break;
                    }
                    else if ('\\' == str.c_str()[i+1])
                    {
                        cBuf[cBufIdx++] = c;
                        cBuf[cBufIdx++] = str.c_str()[++i];
                        break;
                    }
                    
                }  //single slash
                cBuf[cBufIdx++] = '\\';
                cBuf[cBufIdx++] = c;
                break;
            default:
                cBuf[cBufIdx++] = c;
        }
    }
    cBuf[cBufIdx++] = '$';  // implicit trailing anchor
    cBuf[cBufIdx++] = '\0';

#ifdef VERBOSE
      cerr << "regexified string is " << cBuf << endl;
#endif
    return cBuf;
}

} // namespace dataconvert

#undef EXPORT

#endif //DATACONVERT_H

