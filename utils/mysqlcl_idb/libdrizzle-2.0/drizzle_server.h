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
 * @brief Drizzle Declarations for Servers
 */

#include <libdrizzle-2.0/drizzle.h>
#include <libdrizzle-2.0/conn_server.h>
#include <libdrizzle-2.0/handshake_server.h>
#include <libdrizzle-2.0/command_server.h>
#include <libdrizzle-2.0/result_server.h>
#include <libdrizzle-2.0/column_server.h>
#include <libdrizzle-2.0/row_server.h>
#include <libdrizzle-2.0/field_server.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @defgroup drizzle_server_interface Drizzle Server Interface
 */

/**
 * @addtogroup drizzle_server Drizzle Declarations for Servers
 * @ingroup drizzle_server_interface
 * @{
 */

/**
 * Add TCP (IPv4 or IPv6) connection for listening with common arguments.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @param[in] con Caller allocated structure, or NULL to allocate one.
 * @param[in] host Host to listen on. This may be a hostname to resolve, an
 *  IPv4 address, or an IPv6 address. This is passed directly to getaddrinfo().
 * @param[in] port Port to connect to.
 * @param[in] backlog Number of backlog connections passed to listen().
 * @param[in] options Drizzle connection options to add.
 * @return Same return as drizzle_con_create().
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_add_tcp_listen(drizzle_st *drizzle,
                                           const char *host, in_port_t port,
                                           int backlog,
                                           drizzle_con_options_t options);

/**
 * Add unix domain socket connection for listening with common arguments.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @param[in] uds Path to unix domain socket to use for listening.
 * @param[in] backlog Number of backlog connections passed to listen().
 * @param[in] options Drizzle connection options to add.
 * @return Same return as drizzle_con_create().
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_add_uds_listen(drizzle_st *drizzle,
                                           const char *uds, int backlog,
                                           drizzle_con_options_t options);

/**
 * Get next connection marked for listening that is ready for I/O.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @return Connection that is ready to accept, or NULL if there are none.
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_ready_listen(drizzle_st *drizzle);

/**
 * Accept a new connection and initialize the connection structure for it.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @param[out] ret_ptr Standard drizzle return value.
 * @return Same return as drizzle_con_create().
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_accept(drizzle_st *drizzle,
                                   drizzle_return_t *ret_ptr);

/** @} */

#ifdef  __cplusplus
}
#endif
