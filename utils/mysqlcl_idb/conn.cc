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


/**
 * @file
 * @brief Connection Definitions
 */

#include <libdrizzle-2.0/common.h>

/**
 * @addtogroup drizzle_con_static Static Connection Declarations
 * @ingroup drizzle_con
 * @{
 */

/**
 * Set socket options for a connection.
 *
 * @param[in] con Connection structure previously initialized with
 *  drizzle_con_create(), drizzle_con_clone(), or related functions.
 * @return Standard drizzle return value.
 */
static drizzle_return_t _con_setsockopt(drizzle_con_st *con);

/** @} */

/*
 * Common Definitions
 */

int drizzle_con_fd(const drizzle_con_st *con)
{
  return con->fd;
}

drizzle_return_t drizzle_con_set_fd(drizzle_con_st *con, int fd)
{
  con->fd= fd;

  drizzle_return_t ret= _con_setsockopt(con);
  if (ret != DRIZZLE_RETURN_OK)
    con->drizzle->last_errno= errno;

  return ret;
}

void drizzle_con_close(drizzle_con_st *con)
{
  if (con->fd == -1)
    return;

  (void)closesocket(con->fd);
  con->fd= -1;

  con->options&= ~DRIZZLE_CON_READY;
  con->packet_number= 0;
  con->buffer_ptr= con->buffer;
  con->buffer_size= 0;
  con->events= 0;
  con->revents= 0;

  drizzle_state_reset(con);
}

drizzle_return_t drizzle_con_set_events(drizzle_con_st *con, short events)
{
  if ((con->events | events) == con->events)
    return DRIZZLE_RETURN_OK;

  con->events|= events;

  if (con->drizzle->event_watch_fn != NULL)
  {
    drizzle_return_t ret= con->drizzle->event_watch_fn(con, con->events,
                                      con->drizzle->event_watch_context);
    if (ret != DRIZZLE_RETURN_OK)
    {
      drizzle_con_close(con);
      return ret;
    }
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_con_set_revents(drizzle_con_st *con, short revents)
{
  if (revents != 0)
    con->options|= DRIZZLE_CON_IO_READY;

  con->revents= revents;

  /* Remove external POLLOUT watch if we didn't ask for it. Otherwise we spin
     forever until another POLLIN state change. This is much more efficient
     than removing POLLOUT on every state change since some external polling
     mechanisms need to use a system call to change flags (like Linux epoll). */
  if (revents & POLLOUT && !(con->events & POLLOUT) &&
      con->drizzle->event_watch_fn != NULL)
  {
    drizzle_return_t ret= con->drizzle->event_watch_fn(con, con->events,
                                      con->drizzle->event_watch_context);
    if (ret != DRIZZLE_RETURN_OK)
    {
      drizzle_con_close(con);
      return ret;
    }
  }

  con->events&= (short)~revents;

  return DRIZZLE_RETURN_OK;
}

drizzle_st *drizzle_con_drizzle(const drizzle_con_st *con)
{
  return con->drizzle;
}

const char *drizzle_con_error(const drizzle_con_st *con)
{
  return drizzle_error(con->drizzle);
}

int drizzle_con_errno(const drizzle_con_st *con)
{
  return drizzle_errno(con->drizzle);
}

uint16_t drizzle_con_error_code(const drizzle_con_st *con)
{
  return drizzle_error_code(con->drizzle);
}

const char *drizzle_con_sqlstate(const drizzle_con_st *con)
{
  return drizzle_sqlstate(con->drizzle);
}

int drizzle_con_options(const drizzle_con_st *con)
{
  return con->options;
}

void drizzle_con_set_options(drizzle_con_st *con, int options)
{
  con->options= options;
}

void drizzle_con_add_options(drizzle_con_st *con, int options)
{
  con->options|= options;

  /* If asking for the experimental Drizzle protocol, clean the MySQL flag. */
  if (con->options & DRIZZLE_CON_EXPERIMENTAL)
  {
    con->options&= ~DRIZZLE_CON_MYSQL;
  }
}

void drizzle_con_remove_options(drizzle_con_st *con, int options)
{
  con->options&= ~options;
}

const char *drizzle_con_host(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  if (con->socket_type == DRIZZLE_CON_SOCKET_TCP)
  {
    if (con->socket.tcp.host == NULL && !(con->options & DRIZZLE_CON_LISTEN))
    {
      return DRIZZLE_DEFAULT_TCP_HOST;
    }

    return con->socket.tcp.host;
  }

  return NULL;
}

in_port_t drizzle_con_port(const drizzle_con_st *con)
{
  if (con->socket_type == DRIZZLE_CON_SOCKET_TCP)
  {
    if (con->socket.tcp.port != 0)
      return con->socket.tcp.port;

    if (con->options & DRIZZLE_CON_MYSQL)
      return DRIZZLE_DEFAULT_TCP_PORT_MYSQL;

    return DRIZZLE_DEFAULT_TCP_PORT;
  }

  return 0;
}

void drizzle_con_set_tcp(drizzle_con_st *con, const char *host, in_port_t port)
{
  drizzle_con_reset_addrinfo(con);

  con->socket_type= DRIZZLE_CON_SOCKET_TCP;

  if (host == NULL)
  {
    con->socket.tcp.host= NULL;
  }
  else
  {
    con->socket.tcp.host= con->socket.tcp.host_buffer;
    strncpy(con->socket.tcp.host, host, NI_MAXHOST);
    con->socket.tcp.host[NI_MAXHOST - 1]= 0;
  }

  con->socket.tcp.port= port;
}

const char *drizzle_con_user(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  return con->user;
}

const char *drizzle_con_password(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  return con->password;
}

void drizzle_con_set_auth(drizzle_con_st *con, const char *user, const char *password)
{
  if (con == NULL)
  {
    return;
  }

  if (user == NULL)
  {
    con->user[0]= 0;
  }
  else
  {
    strncpy(con->user, user, DRIZZLE_MAX_USER_SIZE);
    con->user[DRIZZLE_MAX_USER_SIZE - 1]= 0;
  }

  if (password == NULL)
  {
    con->password[0]= 0;
  }
  else
  {
    strncpy(con->password, password, DRIZZLE_MAX_PASSWORD_SIZE);
    con->password[DRIZZLE_MAX_PASSWORD_SIZE - 1]= 0;
  }
}

const char *drizzle_con_schema(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  return con->schema;
}

const char *drizzle_con_db(const drizzle_con_st *con)
{
  return drizzle_con_schema(con);
}

void drizzle_con_set_schema(drizzle_con_st *con, const char *schema)
{
  if (con == NULL)
  {
    return;
  }

  if (schema == NULL)
  {
    con->schema[0]= 0;
  }
  else
  {
    strncpy(con->schema, schema, DRIZZLE_MAX_DB_SIZE);
    con->schema[DRIZZLE_MAX_DB_SIZE - 1]= 0;
  }
}

void drizzle_con_set_db(drizzle_con_st *con, const char *schema)
{
  drizzle_con_set_schema(con, schema);
}

void *drizzle_con_context(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  return con->context;
}

void drizzle_con_set_context(drizzle_con_st *con, void *context)
{
  if (con == NULL)
  {
    return;
  }

  con->context= context;
}

void drizzle_con_set_context_free_fn(drizzle_con_st *con,
                                     drizzle_con_context_free_fn *function)
{
  if (con == NULL)
  {
    return;
  }

  con->context_free_fn= function;
}

uint8_t drizzle_con_protocol_version(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->protocol_version;
}

const char *drizzle_con_server_version(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return NULL;
  }

  return con->server_version;
}

uint32_t drizzle_con_server_version_number(const drizzle_con_st *con)
{
  if (con == NULL || con->server_version)
  {
    return 0;
  }

  const char *current= con->server_version;

  char *end;
  uint32_t major= (uint32_t)strtoul(current, &end, 10);
  current= end + 1;

  uint32_t minor= (uint32_t)strtoul(current, &end, 10);
  current= end + 1;

  uint32_t version= (uint32_t)strtoul(current, &end, 10);

  return (major * 10000) + (minor * 100) + version;
}

uint32_t drizzle_con_thread_id(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->thread_id;
}

const uint8_t *drizzle_con_scramble(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->scramble;
}

int drizzle_con_capabilities(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->capabilities;
}

drizzle_charset_t drizzle_con_charset(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return drizzle_charset_t();
  }

  return con->charset;
}

drizzle_con_status_t drizzle_con_status(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return drizzle_con_status_t();
  }

  return con->status;
}

uint32_t drizzle_con_max_packet_size(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->max_packet_size;
}

/*
 * Client Definitions
 */

drizzle_return_t drizzle_con_connect(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (con->options & DRIZZLE_CON_READY)
  {
    return DRIZZLE_RETURN_OK;
  }

  if (drizzle_state_none(con))
  {
    if (!(con->options & DRIZZLE_CON_RAW_PACKET))
    {
      drizzle_state_push(con, drizzle_state_handshake_server_read);
      drizzle_state_push(con, drizzle_state_packet_read);
    }

    drizzle_state_push(con, drizzle_state_connect);
    drizzle_state_push(con, drizzle_state_addrinfo);
  }

  return drizzle_state_loop(con);
}

drizzle_result_st *drizzle_con_quit(drizzle_con_st *con,
                                    drizzle_result_st *result,
                                    drizzle_return_t *ret_ptr)
{
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_QUIT, NULL, 0,
                                   0, ret_ptr);
}

drizzle_result_st *drizzle_quit(drizzle_con_st *con,
                                drizzle_result_st *result,
                                drizzle_return_t *ret_ptr)
{
  return drizzle_con_quit(con, result, ret_ptr);
}

drizzle_result_st *drizzle_con_select_db(drizzle_con_st *con,
                                         drizzle_result_st *result,
                                         const char *db,
                                         drizzle_return_t *ret_ptr)
{
  drizzle_con_set_db(con, db);
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_INIT_DB,
                                   db, strlen(db), strlen(db), ret_ptr);
}

drizzle_result_st *drizzle_select_db(drizzle_con_st *con,
                                     drizzle_result_st *result,
                                     const char *db,
                                     drizzle_return_t *ret_ptr)
{
  return drizzle_con_select_db(con, result, db, ret_ptr);
}

drizzle_result_st *drizzle_con_shutdown(drizzle_con_st *con,
                                        drizzle_result_st *result,
                                        drizzle_return_t *ret_ptr)
{
  if (con == NULL)
  {
    return NULL;
  }

  if (con->options & DRIZZLE_CON_MYSQL)
  {
    return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_SHUTDOWN,
                                     "0", 1, 1, ret_ptr);
  }

  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_SHUTDOWN, NULL,
                                   0, 0, ret_ptr);
}

drizzle_result_st *drizzle_shutdown(drizzle_con_st *con,
                                    drizzle_result_st *result, uint32_t,
                                    drizzle_return_t *ret_ptr)
{
  return drizzle_con_shutdown(con, result, ret_ptr);
}

drizzle_result_st *drizzle_kill(drizzle_con_st *con,
                                drizzle_result_st *result,
                                uint32_t query_id,
                                drizzle_return_t *ret_ptr)
{
  uint32_t sent= htonl(query_id);
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_PROCESS_KILL,
                                   &sent, sizeof(uint32_t), sizeof(uint32_t), ret_ptr);
}

drizzle_result_st *drizzle_con_ping(drizzle_con_st *con,
                                    drizzle_result_st *result,
                                    drizzle_return_t *ret_ptr)
{
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_PING, NULL, 0,
                                   0, ret_ptr);
}

drizzle_result_st *drizzle_ping(drizzle_con_st *con,
                                drizzle_result_st *result,
                                drizzle_return_t *ret_ptr)
{
  return drizzle_con_ping(con, result, ret_ptr);
}

drizzle_result_st *drizzle_con_command_write(drizzle_con_st *con,
                                             drizzle_result_st *result,
                                             drizzle_command_t command,
                                             const void *data, size_t size,
                                             size_t total,
                                             drizzle_return_t *ret_ptr)
{
  if (con == NULL)
  {
    return NULL;
  }

  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  if ((con->options & DRIZZLE_CON_READY) == 0)
  {
    if (con->options & DRIZZLE_CON_RAW_PACKET)
    {
      drizzle_set_error(con->drizzle, "drizzle_command_write", "connection not ready");
      *ret_ptr= DRIZZLE_RETURN_NOT_READY;
      return NULL;
    }

    *ret_ptr= drizzle_con_connect(con);
    if (*ret_ptr != DRIZZLE_RETURN_OK)
    {
      return NULL;
    }
  }

  if (drizzle_state_none(con))
  {
    if (con->options & (DRIZZLE_CON_RAW_PACKET | DRIZZLE_CON_NO_RESULT_READ))
    {
      con->result= NULL;
    }
    else
    {
      for (drizzle_result_st *old_result= con->result_list; old_result != NULL; old_result= old_result->next)
      {
        if (result == old_result)
        {
          drizzle_set_error(con->drizzle, "drizzle_command_write", "result struct already in use");
          *ret_ptr= DRIZZLE_RETURN_INTERNAL_ERROR;
          return NULL;
        }
      }

      con->result= drizzle_result_create_with(con, result);
      if (con->result == NULL)
      {
        *ret_ptr= DRIZZLE_RETURN_MEMORY;
        return NULL;
      }
    }

    con->command= command;
    con->command_data= (uint8_t *)data;
    con->command_size= size;
    con->command_offset= 0;
    con->command_total= total;

    drizzle_state_push(con, drizzle_state_command_write);
  }
  else if (con->command_data == NULL)
  {
    con->command_data= (uint8_t *)data;
    con->command_size= size;
  }

  *ret_ptr= drizzle_state_loop(con);
  if (*ret_ptr == DRIZZLE_RETURN_PAUSE)
  {
    *ret_ptr= DRIZZLE_RETURN_OK;
  }
  else if (*ret_ptr != DRIZZLE_RETURN_OK &&
           *ret_ptr != DRIZZLE_RETURN_IO_WAIT &&
           *ret_ptr != DRIZZLE_RETURN_ERROR_CODE)
  {
    drizzle_result_free(con->result);
    con->result= result;
  }

  return con->result;
}

/*
 * Server Definitions
 */

drizzle_return_t drizzle_con_listen(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  if (con->options & DRIZZLE_CON_READY)
  {
    return DRIZZLE_RETURN_OK;
  }

  if (drizzle_state_none(con))
  {
    drizzle_state_push(con, drizzle_state_listen);
    drizzle_state_push(con, drizzle_state_addrinfo);
  }

  return drizzle_state_loop(con);
}

int drizzle_con_backlog(const drizzle_con_st *con)
{
  if (con == NULL)
  {
    return 0;
  }

  return con->backlog;
}

void drizzle_con_set_backlog(drizzle_con_st *con, int backlog)
{
  if (con == NULL)
  {
    return;
  }

  con->backlog= backlog;
}

void drizzle_con_set_protocol_version(drizzle_con_st *con, uint8_t protocol_version)
{
  if (con == NULL)
  {
    return;
  }

  con->protocol_version= protocol_version;
}

void drizzle_con_set_server_version(drizzle_con_st *con,
                                    const char *server_version)
{
  if (con == NULL)
  {
    return;
  }

  if (server_version == NULL)
  {
    con->server_version[0]= 0;
  }
  else
  {
    strncpy(con->server_version, server_version,
            DRIZZLE_MAX_SERVER_VERSION_SIZE);
    con->server_version[DRIZZLE_MAX_SERVER_VERSION_SIZE - 1]= 0;
  }
}

void drizzle_con_set_thread_id(drizzle_con_st *con, uint32_t thread_id)
{
  if (con == NULL)
  {
    return;
  }

  con->thread_id= thread_id;
}

void drizzle_con_set_scramble(drizzle_con_st *con, const uint8_t *scramble)
{
  if (con == NULL)
  {
    return;
  }

  if (scramble == NULL)
  {
    con->scramble= NULL;
  }
  else
  {
    con->scramble= con->scramble_buffer;
    memcpy(con->scramble, scramble, DRIZZLE_MAX_SCRAMBLE_SIZE);
  }
}

void drizzle_con_set_capabilities(drizzle_con_st *con,
                                  int capabilities)
{
  if (con == NULL)
  {
    return;
  }

  con->capabilities= capabilities;
}

void drizzle_con_set_charset(drizzle_con_st *con, drizzle_charset_t charset)
{
  if (con == NULL)
  {
    return;
  }

  con->charset= charset;
}

void drizzle_con_set_status(drizzle_con_st *con, drizzle_con_status_t status)
{
  if (con == NULL)
  {
    return;
  }

  con->status= status;
}

void drizzle_con_set_max_packet_size(drizzle_con_st *con,
                                     uint32_t max_packet_size)
{
  if (con == NULL)
  {
    return;
  }

  con->max_packet_size= max_packet_size;
}

void drizzle_con_copy_handshake(drizzle_con_st *con, drizzle_con_st *from)
{
  if (con == NULL)
  {
    return;
  }

  drizzle_con_set_auth(con, from->user, NULL);
  drizzle_con_set_scramble(con, from->scramble);
  drizzle_con_set_db(con, from->schema);
  drizzle_con_set_protocol_version(con, from->protocol_version);
  drizzle_con_set_server_version(con, from->server_version);
  drizzle_con_set_thread_id(con, from->thread_id);
  drizzle_con_set_scramble(con, from->scramble);
  drizzle_con_set_capabilities(con, from->capabilities);
  drizzle_con_set_charset(con, from->charset);
  drizzle_con_set_status(con, from->status);
  drizzle_con_set_max_packet_size(con, from->max_packet_size);
}

void *drizzle_con_command_read(drizzle_con_st *con,
                               drizzle_command_t *command, size_t *offset,
                               size_t *size, size_t *total,
                               drizzle_return_t *ret_ptr)
{
  if (con == NULL)
  {
    return NULL;
  }

  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  if (drizzle_state_none(con))
  {
    con->packet_number= 0;
    con->command_offset= 0;
    con->command_total= 0;

    drizzle_state_push(con, drizzle_state_command_read);
    drizzle_state_push(con, drizzle_state_packet_read);
  }

  *offset= con->command_offset;

  *ret_ptr= drizzle_state_loop(con);
  if (*ret_ptr == DRIZZLE_RETURN_PAUSE)
    *ret_ptr= DRIZZLE_RETURN_OK;

  *command= con->command;
  *size= con->command_size;
  *total= con->command_total;

  return con->command_data;
}

void *drizzle_con_command_buffer(drizzle_con_st *con,
                                 drizzle_command_t *command, size_t *total,
                                 drizzle_return_t *ret_ptr)
{
  size_t offset= 0;
  size_t size= 0;

  if (con == NULL)
  {
    return NULL;
  }

  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  size_t total_unused;
  if (total == NULL)
  {
    total= &total_unused;
  }

  uint8_t *command_data=
    static_cast<uint8_t *>(drizzle_con_command_read(con, command, &offset,
                                                    &size, total, ret_ptr));
  if (*ret_ptr != DRIZZLE_RETURN_OK)
  {
    return NULL;
  }

  if (command_data == NULL)
  {
    *total= 0;
    return NULL;
  }

  if (con->command_buffer == NULL)
  {
    con->command_buffer= new (std::nothrow) uint8_t[(*total) + 1];

    if (con->command_buffer == NULL)
    {
      *total= 0;
      *ret_ptr= DRIZZLE_RETURN_MEMORY;
      return NULL;
    }
  }

  memcpy(con->command_buffer + offset, command_data, size);

  while ((offset + size) != (*total))
  {
    command_data= (uint8_t *)drizzle_con_command_read(con, command, &offset,
                                                      &size, total, ret_ptr);
    if (*ret_ptr != DRIZZLE_RETURN_OK)
    {
      return NULL;
    }

    memcpy(con->command_buffer + offset, command_data, size);
  }

  command_data= con->command_buffer;
  con->command_buffer= NULL;
  command_data[*total]= 0;

  return command_data;
}

void drizzle_con_command_buffer_free(uint8_t *command_buffer)
{
  delete[] command_buffer;
}

/*
 * Local Definitions
 */

void drizzle_con_reset_addrinfo(drizzle_con_st *con)
{
  switch (con->socket_type)
  {
  case DRIZZLE_CON_SOCKET_TCP:
    if (con->socket.tcp.addrinfo != NULL)
    {
      freeaddrinfo(con->socket.tcp.addrinfo);
      con->socket.tcp.addrinfo= NULL;
    }
    break;

  case DRIZZLE_CON_SOCKET_UDS:
    con->socket.uds.addrinfo.ai_addr= NULL;
    break;

  default:
    break;
  }

  con->addrinfo_next= NULL;
}

/*
 * State Definitions
 */

drizzle_return_t drizzle_state_addrinfo(drizzle_con_st *con)
{
  drizzle_con_tcp_st *tcp;
  const char *host;
  char port[NI_MAXSERV];
  struct addrinfo ai;
  int ret;

  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  drizzle_log_debug(con->drizzle, "drizzle_state_addrinfo");

  switch (con->socket_type)
  {
  case DRIZZLE_CON_SOCKET_TCP:
    tcp= &(con->socket.tcp);

    if (tcp->addrinfo != NULL)
    {
      freeaddrinfo(tcp->addrinfo);
      tcp->addrinfo= NULL;
    }

    if (tcp->port != 0)
    {
      snprintf(port, NI_MAXSERV, "%u", tcp->port);
    }
    else if (con->options & DRIZZLE_CON_MYSQL)
    {
      snprintf(port, NI_MAXSERV, "%u", DRIZZLE_DEFAULT_TCP_PORT_MYSQL);
    }
    else
    {
      snprintf(port, NI_MAXSERV, "%u", DRIZZLE_DEFAULT_TCP_PORT);
    }
    port[NI_MAXSERV-1]= 0;

    memset(&ai, 0, sizeof(ai));
    ai.ai_socktype= SOCK_STREAM;
    ai.ai_protocol= IPPROTO_TCP;
    ai.ai_flags = AI_PASSIVE;
    ai.ai_family = AF_UNSPEC;

    if (con->options & DRIZZLE_CON_LISTEN)
    {
      host= tcp->host;
    }
    else
    {
      if (tcp->host == NULL)
        host= DRIZZLE_DEFAULT_TCP_HOST;
      else
        host= tcp->host;
    }

    ret= getaddrinfo(host, port, &ai, &(tcp->addrinfo));
    if (ret != 0)
    {
      drizzle_set_error(con->drizzle, "drizzle_state_addrinfo",
                        "getaddrinfo:%s", gai_strerror(ret));
      return DRIZZLE_RETURN_GETADDRINFO;
    }

    con->addrinfo_next= tcp->addrinfo;

    break;

  case DRIZZLE_CON_SOCKET_UDS:
    con->addrinfo_next= &(con->socket.uds.addrinfo);
    break;
  }

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_connect(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }
  drizzle_log_debug(con->drizzle, "drizzle_state_connect");

  if (con->fd != -1)
  {
    (void)closesocket(con->fd);
    con->fd= -1;
  }

  if (con->addrinfo_next == NULL)
  {
    drizzle_set_error(con->drizzle, "drizzle_state_connect",
                      "could not connect");
    drizzle_state_reset(con);
    return DRIZZLE_RETURN_COULD_NOT_CONNECT;
  }

  con->fd= socket(con->addrinfo_next->ai_family,
                  con->addrinfo_next->ai_socktype,
                  con->addrinfo_next->ai_protocol);
  if (con->fd == -1)
  {
    drizzle_set_error(con->drizzle, "drizzle_state_connect", "socket:%d",
                      errno);
    con->drizzle->last_errno= errno;
    return DRIZZLE_RETURN_ERRNO;
  }

  drizzle_return_t dret= _con_setsockopt(con);
  if (dret != DRIZZLE_RETURN_OK)
  {
    con->drizzle->last_errno= errno;
    return dret;
  }

  while (1)
  {
    int ret= connect(con->fd, con->addrinfo_next->ai_addr, con->addrinfo_next->ai_addrlen);

#ifdef _WIN32
#define EINPROGRESS 1
#define ECONNREFUSED 2
#define ENETUNREACH 3
#define ETIMEDOUT 4
#define ECONNRESET 5
#define EADDRINUSE 6
#define EOPNOTSUPP 7
#define ENOPROTOOPT 8
    errno = WSAGetLastError();
    switch(errno) {
    case WSAEINVAL:
    case WSAEALREADY:
    case WSAEWOULDBLOCK:
      errno= EINPROGRESS;
      break;
    case WSAECONNREFUSED:
      errno= ECONNREFUSED;
      break;
    case WSAENETUNREACH:
      errno= ENETUNREACH;
      break;
    case WSAETIMEDOUT:
      errno= ETIMEDOUT;
      break;
    case WSAECONNRESET:
      errno= ECONNRESET;
      break;
    case WSAEADDRINUSE:
      errno= EADDRINUSE;
      break;
    case WSAEOPNOTSUPP:
      errno= EOPNOTSUPP;
      break;
    case WSAENOPROTOOPT:
      errno= ENOPROTOOPT;
      break;
    default:
      break;
    }
#endif /* _WIN32 */
	
    drizzle_log_crazy(con->drizzle, "connect return=%d errno=%d", ret, errno);

    if (ret == 0)
    {
      con->addrinfo_next= NULL;
      break;
    }

    if (errno == EAGAIN || errno == EINTR)
      continue;

    if (errno == EINPROGRESS)
    {
      drizzle_state_pop(con);
      drizzle_state_push(con, drizzle_state_connecting);
      return DRIZZLE_RETURN_OK;
    }

    if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == ETIMEDOUT)
    {
      con->addrinfo_next= con->addrinfo_next->ai_next;
      return DRIZZLE_RETURN_OK;
    }

    drizzle_set_error(con->drizzle, "drizzle_state_connect", "connect:%d",
                      errno);
    con->drizzle->last_errno= errno;
    return DRIZZLE_RETURN_ERRNO;
  }

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_connecting(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }
  drizzle_log_debug(con->drizzle, "drizzle_state_connecting");

  while (1)
  {
    if (con->revents & POLLOUT)
    {
      drizzle_state_pop(con);
      return DRIZZLE_RETURN_OK;
    }
    else if (con->revents & (POLLERR | POLLHUP | POLLNVAL))
    {
      con->revents= 0;
      drizzle_state_pop(con);
      drizzle_state_push(con, drizzle_state_connect);
      con->addrinfo_next= con->addrinfo_next->ai_next;
      return DRIZZLE_RETURN_OK;
    }

    drizzle_return_t ret= drizzle_con_set_events(con, POLLOUT);
    if (ret != DRIZZLE_RETURN_OK)
      return ret;

    if (con->drizzle->options.is_non_blocking)
    {
      return DRIZZLE_RETURN_IO_WAIT;
    }

    ret= drizzle_con_wait(con->drizzle);
    if (ret != DRIZZLE_RETURN_OK)
    {
      return ret;
    }
  }
}

drizzle_return_t drizzle_state_read(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  drizzle_log_debug(con->drizzle, "drizzle_state_read");

  if (con->buffer_size == 0)
    con->buffer_ptr= con->buffer;
  else if ((con->buffer_ptr - con->buffer) > (DRIZZLE_MAX_BUFFER_SIZE / 2))
  {
    memmove(con->buffer, con->buffer_ptr, con->buffer_size);
    con->buffer_ptr= con->buffer;
  }

  if ((con->revents & POLLIN) == 0 &&
      (con->drizzle->options.is_non_blocking))
  {
    /* non-blocking mode: return IO_WAIT instead of attempting to read. This
     * avoids reading immediately after writing a command, which typically
     * returns EAGAIN. This improves performance. */
    drizzle_return_t ret= drizzle_con_set_events(con, POLLIN);
    if (ret != DRIZZLE_RETURN_OK)
    {
      return ret;
    }
    return DRIZZLE_RETURN_IO_WAIT;
  }

  while (1)
  {
    size_t available_buffer= (size_t)DRIZZLE_MAX_BUFFER_SIZE -
        ((size_t)(con->buffer_ptr - con->buffer) + con->buffer_size);
    ssize_t read_size = recv(con->fd, (char *)con->buffer_ptr + con->buffer_size,
                     available_buffer, 0);
#ifdef _WIN32
    errno = WSAGetLastError();
    switch(errno) {
    case WSAENOTCONN:
    case WSAEWOULDBLOCK:
      errno= EAGAIN;
      break;
    case WSAEINVAL:
    case WSAEALREADY:
      errno= EINPROGRESS;
      break;
    case WSAECONNREFUSED:
      errno= ECONNREFUSED;
      break;
    case WSAENETUNREACH:
      errno= ENETUNREACH;
      break;
    case WSAETIMEDOUT:
      errno= ETIMEDOUT;
      break;
    case WSAECONNRESET:
      errno= ECONNRESET;
      break;
    case WSAEADDRINUSE:
      errno= EADDRINUSE;
      break;
    case WSAEOPNOTSUPP:
      errno= EOPNOTSUPP;
      break;
    case WSAENOPROTOOPT:
      errno= ENOPROTOOPT;
      break;
    default:
      break;
    }
#endif /* _WIN32 */	

    drizzle_log_crazy(con->drizzle, "read fd=%d return=%zd errno=%d", con->fd,
                      read_size, errno);

    if (read_size == 0)
    {
      drizzle_set_error(con->drizzle, "drizzle_state_read",
                        "lost connection to server (EOF)");
      return DRIZZLE_RETURN_LOST_CONNECTION;
    }
    else if (read_size == -1)
    {
      if (errno == EAGAIN)
      {
        /* clear the read ready flag */
        con->revents&= ~POLLIN;
        drizzle_return_t ret= drizzle_con_set_events(con, POLLIN);
        if (ret != DRIZZLE_RETURN_OK)
        {
          return ret;
        }

        if (con->drizzle->options.is_non_blocking)
        {
          return DRIZZLE_RETURN_IO_WAIT;
        }

        ret= drizzle_con_wait(con->drizzle);
        if (ret != DRIZZLE_RETURN_OK)
        {
          return ret;
        }

        continue;
      }
      else if (errno == ECONNREFUSED)
      {
        con->revents= 0;
        drizzle_state_pop(con);
        drizzle_state_push(con, drizzle_state_connect);
        con->addrinfo_next= con->addrinfo_next->ai_next;
        return DRIZZLE_RETURN_OK;
      }
      else if (errno == EINTR)
        continue;
      else if (errno == EPIPE || errno == ECONNRESET)
      {
        drizzle_set_error(con->drizzle, "drizzle_state_read",
                          "lost connection to server (%d)", errno);
        return DRIZZLE_RETURN_LOST_CONNECTION;
      }

      drizzle_set_error(con->drizzle, "drizzle_state_read", "read:%d", errno);
      con->drizzle->last_errno= errno;
      return DRIZZLE_RETURN_ERRNO;
    }

    /* clear the "read ready" flag if we read all available data. */
    if ((size_t) read_size < available_buffer) con->revents&= ~POLLIN;
    con->buffer_size+= (size_t)read_size;
    break;
  }

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_write(drizzle_con_st *con)
{
  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  drizzle_log_debug(con->drizzle, "drizzle_state_write");

  while (con->buffer_size != 0)
  {
    ssize_t write_size = send(con->fd,(char *) con->buffer_ptr, con->buffer_size, 0);

#ifdef _WIN32
    errno = WSAGetLastError();
    switch(errno) {
    case WSAENOTCONN:
    case WSAEWOULDBLOCK:
      errno= EAGAIN;
      break;
    case WSAEINVAL:
    case WSAEALREADY:
      errno= EINPROGRESS;
      break;
    case WSAECONNREFUSED:
      errno= ECONNREFUSED;
      break;
    case WSAENETUNREACH:
      errno= ENETUNREACH;
      break;
    case WSAETIMEDOUT:
      errno= ETIMEDOUT;
      break;
    case WSAECONNRESET:
      errno= ECONNRESET;
      break;
    case WSAEADDRINUSE:
      errno= EADDRINUSE;
      break;
    case WSAEOPNOTSUPP:
      errno= EOPNOTSUPP;
      break;
    case WSAENOPROTOOPT:
      errno= ENOPROTOOPT;
      break;
    default:
      break;
    }
#endif /* _WIN32 */	

    drizzle_log_crazy(con->drizzle, "write fd=%d return=%zd errno=%d", con->fd,
                      write_size, errno);

    if (write_size == 0)
    {
      drizzle_set_error(con->drizzle, "drizzle_state_write",
                        "lost connection to server (EOF)");
      return DRIZZLE_RETURN_LOST_CONNECTION;
    }
    else if (write_size == -1)
    {
      if (errno == EAGAIN)
      {
        drizzle_return_t ret= drizzle_con_set_events(con, POLLOUT);
        if (ret != DRIZZLE_RETURN_OK)
        {
          return ret;
        }

        if (con->drizzle->options.is_non_blocking)
        {
          return DRIZZLE_RETURN_IO_WAIT;
        }

        ret= drizzle_con_wait(con->drizzle);
        if (ret != DRIZZLE_RETURN_OK)
        {
          return ret;
        }

        continue;
      }
      else if (errno == EINTR)
      {
        continue;
      }
      else if (errno == EPIPE || errno == ECONNRESET)
      {
        drizzle_set_error(con->drizzle, "drizzle_state_write",
                          "lost connection to server (%d)", errno);
        return DRIZZLE_RETURN_LOST_CONNECTION;
      }

      drizzle_set_error(con->drizzle, "drizzle_state_write", "write:%d", errno);
      con->drizzle->last_errno= errno;
      return DRIZZLE_RETURN_ERRNO;
    }

    con->buffer_ptr+= write_size;
    con->buffer_size-= (size_t)write_size;
    if (con->buffer_size == 0)
      break;
  }

  con->buffer_ptr= con->buffer;

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_listen(drizzle_con_st *con)
{
  char host[NI_MAXHOST];
  char port[NI_MAXSERV];
  int ret;
  int fd;
  int opt;
  drizzle_con_st *new_con;

  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }


  for (; con->addrinfo_next != NULL;
       con->addrinfo_next= con->addrinfo_next->ai_next)
  {
    ret= getnameinfo(con->addrinfo_next->ai_addr,
                     con->addrinfo_next->ai_addrlen, host, NI_MAXHOST, port,
                     NI_MAXSERV, NI_NUMERICHOST | NI_NUMERICSERV);
    if (ret != 0)
    {
      drizzle_set_error(con->drizzle, "drizzle_state_listen", "getnameinfo:%s",
                        gai_strerror(ret));
      return DRIZZLE_RETURN_GETADDRINFO;
    }

    /* Call to socket() can fail for some getaddrinfo results, try another. */
    fd= socket(con->addrinfo_next->ai_family, con->addrinfo_next->ai_socktype,
               con->addrinfo_next->ai_protocol);
    if (fd == -1)
    {
      drizzle_log_info(con->drizzle, "could not listen on %s:%s", host, port);
      drizzle_set_error(con->drizzle, "drizzle_state_listen", "socket:%d",
                        errno);
      continue;
    }
	
	opt= 1;
#ifdef _WIN32
        ret= setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,(const char*) &opt, sizeof(opt));
#else
        ret= setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif /* _WIN32 */
    if (ret == -1)
    {
      closesocket(fd);
      drizzle_set_error(con->drizzle, "drizzle_state_listen", "setsockopt:%d",
                        errno);
      return DRIZZLE_RETURN_ERRNO;
    }

    ret= bind(fd, con->addrinfo_next->ai_addr, con->addrinfo_next->ai_addrlen);
    if (ret == -1)
    {
      closesocket(fd);
      drizzle_set_error(con->drizzle, "drizzle_state_listen", "bind:%d", errno);
      if (errno == EADDRINUSE)
      {
        if (con->fd == -1)
        {
          drizzle_log_info(con->drizzle, "could not listen on %s:%s", host,
                           port);
        }

        continue;
      }

      return DRIZZLE_RETURN_ERRNO;
    }

    if (listen(fd, con->backlog) == -1)
    {
      closesocket(fd);
      drizzle_set_error(con->drizzle, "drizzle_state_listen", "listen:%d",
                        errno);
      return DRIZZLE_RETURN_ERRNO;
    }

    if (con->fd == -1)
    {
      con->fd= fd;
      new_con= con;
    }
    else
    {
      new_con= drizzle_con_clone(con->drizzle, con);
      if (new_con == NULL)
      {
        closesocket(fd);
        return DRIZZLE_RETURN_MEMORY;
      }

      new_con->fd= fd;
    }

    /* Wait for read events on the listening socket. */
    drizzle_return_t driz_ret= drizzle_con_set_events(new_con, POLLIN);
    if (ret != DRIZZLE_RETURN_OK)
    {
      drizzle_con_free(new_con);
      return driz_ret;
    }

    drizzle_log_info(con->drizzle, "listening on %s:%s", host, port);
  }

  /* Report last socket() error if we couldn't find an address to bind. */
  if (con->fd == -1)
  {
    return DRIZZLE_RETURN_ERRNO;
  }

  drizzle_state_pop(con);
  return DRIZZLE_RETURN_OK;
}

/*
 * Static Definitions
 */

static drizzle_return_t _con_setsockopt(drizzle_con_st *con)
{
  int ret;
  struct linger linger;
  struct timeval waittime;

  if (con == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }


  ret= 1;

#ifdef _WIN32
  ret= setsockopt(con->fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&ret,
                  (socklen_t)sizeof(int));
#else
  ret= setsockopt(con->fd, IPPROTO_TCP, TCP_NODELAY, &ret,
                  (socklen_t)sizeof(int));
#endif /* _WIN32 */

  if (ret == -1 && errno != EOPNOTSUPP)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:TCP_NODELAY:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

  linger.l_onoff= 1;
  linger.l_linger= DRIZZLE_DEFAULT_SOCKET_TIMEOUT;

#ifdef _WIN32
  ret= setsockopt(con->fd, SOL_SOCKET, SO_LINGER, (const char*)&linger,
                  (socklen_t)sizeof(struct linger));
#else
  ret= setsockopt(con->fd, SOL_SOCKET, SO_LINGER, &linger,
                  (socklen_t)sizeof(struct linger));
#endif /* _WIN32 */

  if (ret == -1)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:SO_LINGER:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

  waittime.tv_sec= DRIZZLE_DEFAULT_SOCKET_TIMEOUT;
  waittime.tv_usec= 0;

#ifdef _WIN32
  ret= setsockopt(con->fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&waittime,
                  (socklen_t)sizeof(struct timeval));
#else
  ret= setsockopt(con->fd, SOL_SOCKET, SO_SNDTIMEO, &waittime,
                  (socklen_t)sizeof(struct timeval));
#endif /* _WIN32 */

  if (ret == -1 && errno != ENOPROTOOPT)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:SO_SNDTIMEO:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

#ifdef _WIN32
  ret= setsockopt(con->fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&waittime,
                  (socklen_t)sizeof(struct timeval));
#else
  ret= setsockopt(con->fd, SOL_SOCKET, SO_RCVTIMEO, &waittime,
                  (socklen_t)sizeof(struct timeval));
#endif /* _WIN32 */

  if (ret == -1 && errno != ENOPROTOOPT)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:SO_RCVTIMEO:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

  ret= DRIZZLE_DEFAULT_SOCKET_SEND_SIZE;
#ifdef _WIN32
  ret= setsockopt(con->fd, SOL_SOCKET, SO_SNDBUF, (const char*)&ret, (socklen_t)sizeof(int));
#else
  ret= setsockopt(con->fd, SOL_SOCKET, SO_SNDBUF, &ret, (socklen_t)sizeof(int));
#endif /* _WIN32 */
  if (ret == -1)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:SO_SNDBUF:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

  ret= DRIZZLE_DEFAULT_SOCKET_RECV_SIZE;
#ifdef _WIN32
  ret= setsockopt(con->fd, SOL_SOCKET, SO_RCVBUF, (const char*)&ret, (socklen_t)sizeof(int));
#else
  ret= setsockopt(con->fd, SOL_SOCKET, SO_RCVBUF, &ret, (socklen_t)sizeof(int));
#endif /* _WIN32 */
  if (ret == -1)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt",
                      "setsockopt:SO_RCVBUF:%d", errno);
    return DRIZZLE_RETURN_ERRNO;
  }

#if defined (_WIN32)
  {
    unsigned long asyncmode;
    asyncmode= 1;
    ioctlsocket(con->fd, FIONBIO, &asyncmode);
  }
#else
  ret= fcntl(con->fd, F_GETFL, 0);
  if (ret == -1)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt", "fcntl:F_GETFL:%d",
                      errno);
    return DRIZZLE_RETURN_ERRNO;
  }

  ret= fcntl(con->fd, F_SETFL, ret | O_NONBLOCK);
  if (ret == -1)
  {
    drizzle_set_error(con->drizzle, "_con_setsockopt", "fcntl:F_SETFL:%d",
                      errno);
    return DRIZZLE_RETURN_ERRNO;
  }
#endif

  return DRIZZLE_RETURN_OK;
}
