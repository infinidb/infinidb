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
 * @brief Connection Definitions for Unix Domain Sockets
 */

#include <libdrizzle-2.0/common.h>

const char *drizzle_con_uds(const drizzle_con_st *con)
{
  if (con->socket_type == DRIZZLE_CON_SOCKET_UDS)
  {
    if (con->socket.uds.sockaddr.sun_path[0] != 0)
      return con->socket.uds.sockaddr.sun_path;

    if (con->options & DRIZZLE_CON_MYSQL)
      return DRIZZLE_DEFAULT_UDS_MYSQL;

    return DRIZZLE_DEFAULT_UDS;
  }

  return NULL;
}

void drizzle_con_set_uds(drizzle_con_st *con, const char *uds)
{
  drizzle_con_reset_addrinfo(con);

  con->socket_type= DRIZZLE_CON_SOCKET_UDS;

  if (uds == NULL)
    uds= "";

  con->socket.uds.sockaddr.sun_family= AF_UNIX;
  strncpy(con->socket.uds.sockaddr.sun_path, uds,
          sizeof(con->socket.uds.sockaddr.sun_path));
  con->socket.uds.sockaddr.sun_path[sizeof(con->socket.uds.sockaddr.sun_path) - 1]= 0;

  con->socket.uds.addrinfo.ai_family= AF_UNIX;
  con->socket.uds.addrinfo.ai_socktype= SOCK_STREAM;
  con->socket.uds.addrinfo.ai_protocol= 0;
  con->socket.uds.addrinfo.ai_addrlen= sizeof(struct sockaddr_un);
  con->socket.uds.addrinfo.ai_addr= (struct sockaddr *)&(con->socket.uds.sockaddr);
}
