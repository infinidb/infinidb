/*
 * Drizzle Client & Protocol Library
 *
 * Copyright (C) 2008 Eric Day (eday@oddments.org)
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
 *
 */

#pragma once

/**
 * @file
 * @brief Struct Definitions
 */

#include <sys/types.h>

#ifndef NI_MAXHOST
# define NI_MAXHOST 1025
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __cplusplus
struct drizzle_st;
struct drizzle_con_tcp_st;
struct drizzle_con_uds_st;
struct drizzle_con_st;
struct drizzle_query_st;
struct drizzle_result_st;
struct drizzle_column_st;
#else


/**
 * @ingroup drizzle
 */
class drizzle_st
{
public:
  uint16_t error_code;
  struct options_t {
    bool is_allocated;
    bool is_non_blocking;
    bool is_free_objects;
    bool is_assert_dangling;

    options_t() :
      is_allocated(false),
      is_non_blocking(false),
      is_free_objects(false),
      is_assert_dangling(false)
    { }
  } options;
  drizzle_verbose_t verbose;
  uint32_t con_count;
  uint32_t pfds_size;
  uint32_t query_count;
  uint32_t query_new;
  uint32_t query_running;
  int last_errno;
  int timeout;
  drizzle_con_st *con_list;
  void *context;
  drizzle_context_free_fn *context_free_fn;
  drizzle_event_watch_fn *event_watch_fn;
  void *event_watch_context;
  drizzle_log_fn *log_fn;
  void *log_context;
  struct pollfd *pfds;
  drizzle_query_st *query_list;
  char sqlstate[DRIZZLE_MAX_SQLSTATE_SIZE + 1];
  char last_error[DRIZZLE_MAX_ERROR_SIZE];

  drizzle_st() :
    error_code(0),
    options(),
    verbose(DRIZZLE_VERBOSE_ERROR),
    con_count(0),
    pfds_size(0),
    query_count(0),
    query_new(0),
    query_running(0),
    last_errno(0),
    timeout(-1),
    con_list(NULL),
    context(NULL),
    context_free_fn(NULL),
    event_watch_fn(NULL),
    event_watch_context(NULL),
    log_fn(NULL),
    log_context(NULL),
    pfds(NULL),
    query_list(NULL)
  { }
};

/**
 * @ingroup drizzle_con
 */
class drizzle_con_tcp_st
{
public:
  in_port_t port;
  struct addrinfo *addrinfo;
  char *host;
  char host_buffer[NI_MAXHOST];
};

/**
 * @ingroup drizzle_con
 */
class drizzle_con_uds_st
{
public:
  struct addrinfo addrinfo;
  struct sockaddr_un sockaddr;
};

/**
 * @ingroup drizzle_con
 */
class drizzle_con_st
{
public:
  uint8_t packet_number;
  uint8_t protocol_version;
  uint8_t state_current;
  short events;
  short revents;
  int capabilities;
  drizzle_charset_t charset;
  drizzle_command_t command;
  struct option_t {
    bool is_allocated;

    option_t() :
      is_allocated(false)
    { }
  } _options;
  int options;
  drizzle_con_socket_t socket_type;
  drizzle_con_status_t status;
  uint32_t max_packet_size;
  uint32_t result_count;
  uint32_t thread_id;
  int backlog;
  int fd;
  size_t buffer_size;
  size_t command_offset;
  size_t command_size;
  size_t command_total;
  size_t packet_size;
  struct addrinfo *addrinfo_next;
  uint8_t *buffer_ptr;
  uint8_t *command_buffer;
  uint8_t *command_data;
  void *context;
  drizzle_con_context_free_fn *context_free_fn;
  drizzle_st *drizzle;
  drizzle_con_st *next;
  drizzle_con_st *prev;
  drizzle_query_st *query;
  drizzle_result_st *result;
  drizzle_result_st *result_list;
  uint8_t *scramble;
  union
  {
    drizzle_con_tcp_st tcp;
    drizzle_con_uds_st uds;
  } socket;
  uint8_t buffer[DRIZZLE_MAX_BUFFER_SIZE];
  char schema[DRIZZLE_MAX_DB_SIZE];
  char password[DRIZZLE_MAX_PASSWORD_SIZE];
  uint8_t scramble_buffer[DRIZZLE_MAX_SCRAMBLE_SIZE];
  char server_version[DRIZZLE_MAX_SERVER_VERSION_SIZE];
  char server_extra[DRIZZLE_MAX_SERVER_EXTRA_SIZE];
  drizzle_state_fn *state_stack[DRIZZLE_STATE_STACK_SIZE];
  char user[DRIZZLE_MAX_USER_SIZE];

  drizzle_con_st() :
    packet_number(0),
    protocol_version(0),
    state_current(0),
    events(0),
    revents(0),
    capabilities(DRIZZLE_CAPABILITIES_NONE),
    options(DRIZZLE_CON_NONE),
    max_packet_size(0),
    result_count(0),
    thread_id(0),
    backlog(0),
    fd(0),
    buffer_size(0),
    command_offset(0),
    command_size(0),
    command_total(0),
    packet_size(0),
    addrinfo_next(NULL),
    buffer_ptr(NULL),
    command_buffer(NULL),
    command_data(NULL),
    context(NULL),
    context_free_fn(NULL),
    drizzle(NULL),
    next(NULL),
    prev(NULL),
    query(NULL),
    result(NULL),
    result_list(NULL),
    scramble(NULL)
  { }
};

/**
 * @ingroup drizzle_query
 */
class drizzle_query_st
{
private:
public:
  drizzle_st *drizzle;
  drizzle_query_st *next;
  drizzle_query_st *prev;
  struct option_t {
    bool is_allocated;

    option_t() :
      is_allocated(false)
    { }
  } options;
  drizzle_query_state_t state;
  drizzle_con_st *con;
  drizzle_result_st *result;
  const char *string;
  size_t size;
  void *context;
  drizzle_query_context_free_fn *context_free_fn;

  drizzle_query_st() :
    drizzle(NULL),
    next(NULL),
    prev(NULL),
    con(NULL),
    result(NULL),
    string(NULL),
    size(0),
    context(NULL),
    context_free_fn(NULL)
  { }
};

/**
 * @ingroup drizzle_result
 */
class drizzle_result_st
{
public:
  drizzle_con_st *con;
  drizzle_result_st *next;
  drizzle_result_st *prev;
  struct option_t {
    bool is_allocated;

    option_t() :
      is_allocated(false)
    { }
  } _options;
  int options;

  char info[DRIZZLE_MAX_INFO_SIZE];
  uint16_t error_code;
  char sqlstate[DRIZZLE_MAX_SQLSTATE_SIZE + 1];
  uint64_t insert_id;
  uint16_t warning_count;
  uint64_t affected_rows;

  uint16_t column_count;
  uint16_t column_current;
  drizzle_column_st *column_list;
  drizzle_column_st *column;
  drizzle_column_st *column_buffer;

  uint64_t row_count;
  uint64_t row_current;

  uint16_t field_current;
  size_t field_total;
  size_t field_offset;
  size_t field_size;
  drizzle_field_t field;
  drizzle_field_t field_buffer;

  uint64_t row_list_size;
  drizzle_row_t row;
  drizzle_row_list_t *row_list;
  size_t *field_sizes;
  drizzle_field_sizes_list_t *field_sizes_list;

  drizzle_result_st() :
    con(NULL),
    next(NULL),
    prev(NULL),
    options(DRIZZLE_RESULT_NONE),
    error_code(0),
    insert_id(0),
    warning_count(0),
    affected_rows(0),
    column_count(0),
    column_current(0),
    column_list(NULL),
    column(NULL),
    column_buffer(NULL),
    row_count(0),
    row_current(0),
    field_current(0),
    field_total(0),
    field_offset(0),
    field_size(0),
    row_list_size(0),
    row_list(NULL),
    field_sizes(NULL),
    field_sizes_list(NULL)
  { }
};

/**
 * @ingroup drizzle_column
 */
class drizzle_column_st
{
public:
  drizzle_result_st *result;
  drizzle_column_st *next;
  drizzle_column_st *prev;
  struct options_t {
    bool is_allocated;

    options_t() :
      is_allocated(false)
    { }
  } options;
  char catalog[DRIZZLE_MAX_CATALOG_SIZE];
  char schema[DRIZZLE_MAX_DB_SIZE];
  char table[DRIZZLE_MAX_TABLE_SIZE];
  char orig_table[DRIZZLE_MAX_TABLE_SIZE];
  char name[DRIZZLE_MAX_COLUMN_NAME_SIZE];
  char orig_name[DRIZZLE_MAX_COLUMN_NAME_SIZE];
  drizzle_charset_t charset;
  uint32_t size;
  size_t max_size;
  drizzle_column_type_t type;
  int flags;
  uint8_t decimals;
  uint8_t default_value[DRIZZLE_MAX_DEFAULT_VALUE_SIZE];
  size_t default_value_size;

  drizzle_column_st() :
    result(NULL),
    next(NULL),
    prev(NULL),
    size(0),
    max_size(0),
    flags(DRIZZLE_COLUMN_FLAGS_NONE),
    decimals(0),
    default_value_size(0)
  { }
};
#endif

#ifdef __cplusplus
}
#endif
