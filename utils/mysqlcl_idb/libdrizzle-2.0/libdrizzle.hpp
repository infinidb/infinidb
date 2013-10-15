/* Drizzle Client Library
 * Copyright (C) 2011 Olaf van der Spek
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *     * The names of its contributors may not be used to endorse or
 * promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <cstring>
#include <fstream>
#include <libdrizzle-2.0/libdrizzle.h>
#include <map>
#include <sstream>
#include <stdexcept>

namespace drizzle {

class bad_query : public std::runtime_error
{
public:
  bad_query(const std::string& v) : std::runtime_error(v)
  {
  }
};

class noncopyable
{
protected:
  noncopyable()
  {
  }
private:
  noncopyable(const noncopyable&);
  void operator=(const noncopyable&);
};

class drizzle_c : noncopyable
{
public:
  drizzle_c()
  {
    b_= drizzle_create();
  }

  ~drizzle_c()
  {
    drizzle_free(b_);
  }

  operator drizzle_st*()
  {
    return b_;
  }
private:
  drizzle_st *b_;
};

class result_c
{
public:
  operator drizzle_result_st*()
  {
    if (!b_)
      b_.reset(new drizzle_result_st, drizzle_result_free);
    return b_.get();
  }

  const char* error()
  {
    return drizzle_result_error(*this);
  }

  uint16_t error_code()
  {
    return drizzle_result_error_code(*this);
  }

  uint16_t column_count()
  {
    return drizzle_result_column_count(*this);    
  }

  uint64_t row_count()
  {
    return drizzle_result_row_count(*this);
  }

  drizzle_column_st* column_next()
  {
    return drizzle_column_next(*this);
  }

  drizzle_row_t row_next()
  {
    return drizzle_row_next(*this);
  }

  void column_seek(uint16_t i)
  {
    drizzle_column_seek(*this, i);
  }

  void row_seek(uint64_t i)
  {
    drizzle_row_seek(*this, i);
  }

  size_t* row_field_sizes()
  {
    return drizzle_row_field_sizes(*this);
  }
private:
  boost::shared_ptr<drizzle_result_st> b_;
};

class connection_c : noncopyable
{
public:
  explicit connection_c(drizzle_c& drizzle)
  {
    b_= drizzle_con_create(drizzle);

    if (b_ == NULL)
    {
      throw "drizzle_con_create() failed";
    }
    read_conf_files();
  }

  ~connection_c()
  {
    drizzle_con_free(b_);
  }

  operator drizzle_con_st*()
  {
    return b_;
  }

  const char* error()
  {
    return drizzle_con_error(b_);
  }

  void set_tcp(const char* host, in_port_t port)
  {
    drizzle_con_set_tcp(b_, host, port);
  }

  void set_auth(const char* user, const char* password)
  {
    drizzle_con_set_auth(b_, user, password);
  }

  void set_db(const char* db)
  {
    drizzle_con_set_db(b_, db);
  }

  drizzle_return_t query(result_c& result, const char* str, size_t str_size)
  {
    drizzle_return_t ret;

    drizzle_query(*this, result, str, str_size, &ret);

    if (!ret)
    {
      ret = drizzle_result_buffer(result);
    }

    return ret;
  }

  drizzle_return_t query(result_c& result, const std::string& str)
  {
    return query(result, str.data(), str.size());
  }

  drizzle_return_t query(result_c& result, const char* str)
  {
    return query(result, str, strlen(str));
  }

  result_c query(const char* str, size_t str_size)
  {
    result_c result;
    if (query(result, str, str_size))
    {
      throw bad_query(error());
    }

    return result;
  }

  result_c query(const std::string& str)
  {
    return query(str.data(), str.size());
  }

  result_c query(const char* str)
  {
    return query(str, strlen(str));
  }
private:
  void read_conf_files();

  drizzle_con_st *b_;
};

class query_c
{
public:
  query_c(connection_c& con, const std::string& in = "") :
    con_(con),
    in_(in)
  {
  }

  void operator=(const std::string& v)
  {
    in_ = v;
    out_.clear();
  }

  void operator+=(const std::string& v)
  {
    in_ += v;
  }

  query_c& p_name(const std::string& v)
  {
    std::vector<char> r(2 * v.size() + 2);
    r.resize(drizzle_escape_string(&r.front() + 1, r.size(), v.data(), v.size()) + 2);    
    r.front() = '`';
    r.back() = '`';
    p_raw(&r.front(), r.size());
    return *this;
  }

  query_c& p_raw(const char* v, size_t sz)
  {
    size_t i = in_.find('?');
    assert(i != std::string::npos);
    if (i == std::string::npos)
      return *this;
    out_.append(in_.substr(0, i));
    in_.erase(0, i + 1);
    out_.append(v, sz);
    return *this;
  }

  query_c& p_raw(const std::string& v)
  {
    return p_raw(v.data(), v.size());
  }

  query_c& p(const std::string& v)
  {
    std::vector<char> r(2 * v.size() + 2);
    r.resize(drizzle_escape_string(&r.front() + 1, r.size(), v.data(), v.size()) + 2);    
    r.front() = '\'';
    r.back() = '\'';
    p_raw(&r.front(), r.size());
    return *this;
  }

  query_c& p(long long v)
  {
    std::stringstream ss;
    ss << v;
    p_raw(ss.str());
    return *this;
  }

  drizzle_return_t execute(result_c& result)
  {
    return con_.query(result, read());
  }

  result_c execute()
  {
    return con_.query(read());
  }

  std::string read() const
  {
    return out_ + in_;
  }
private:
  connection_c& con_;
  std::string in_;
  std::string out_;
};

template<class T>
const char* get_conf(const T& c, const std::string& v)
{
  typename T::const_iterator i = c.find(v);
  return i == c.end() ? NULL : i->second.c_str();
}

void connection_c::read_conf_files()
{
  using namespace std;

  vector<string> conf_files;
#ifdef WIN32
  {
    boost::array<char, MAX_PATH> d;
    GetWindowsDirectoryA(d.data(), d.size());
    conf_files.push_back(string(d.data()) + "/my.cnf");
    conf_files.push_back(string(d.data()) + "/drizzle.cnf");
    conf_files.push_back(string(d.data()) + "/drizzle.conf");
  }
#else
  conf_files.push_back("/etc/mysql/my.cnf");
  conf_files.push_back("/etc/drizzle/drizzle.cnf");
  conf_files.push_back("/etc/drizzle/drizzle.conf");
#endif
  if (const char* d = getenv("HOME"))
  {
    conf_files.push_back(string(d) + "/.my.cnf");  
    conf_files.push_back(string(d) + "/.drizzle.conf");
  }
  
  map<string, string> conf;
  BOOST_FOREACH(string& it, conf_files)
  {
    ifstream is(it.c_str());
    bool client_section = false;
    for (string s; getline(is, s); )
    {
      size_t i = s.find('#');
      if (i != string::npos)
        s.erase(i);
      boost::trim(s);
      if (boost::starts_with(s, "["))
      {
        client_section = s == "[client]";
        continue;
      }
      else if (!client_section)
        continue;
      i = s.find('=');
      if (i != string::npos)
        conf[boost::trim_copy(s.substr(0, i))] = boost::trim_copy(s.substr(i + 1));
    }
  }
  if (conf.count("host") || conf.count("port"))
    set_tcp(get_conf(conf, "host"), atoi(get_conf(conf, "port")));
  if (conf.count("user") || conf.count("password"))
    set_auth(get_conf(conf, "user"), get_conf(conf, "password"));
}

}
