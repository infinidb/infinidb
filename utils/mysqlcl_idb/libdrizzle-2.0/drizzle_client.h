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
 * @brief Drizzle Declarations for Clients
 */

#include <libdrizzle-2.0/drizzle.h>
#include <libdrizzle-2.0/conn_client.h>
#include <libdrizzle-2.0/handshake_client.h>
#include <libdrizzle-2.0/command_client.h>
#include <libdrizzle-2.0/query.h>
#include <libdrizzle-2.0/result_client.h>
#include <libdrizzle-2.0/column_client.h>
#include <libdrizzle-2.0/row_client.h>
#include <libdrizzle-2.0/field_client.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @defgroup drizzle_client_interface Drizzle Client Interface
 */

/**
 * @addtogroup drizzle_client Drizzle Declarations for Clients
 * @ingroup drizzle_client_interface
 * @{
 */

/**
 * Add TCP (IPv4 or IPv6) connection with common arguments.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @param[in] con Caller allocated structure, or NULL to allocate one.
 * @param[in] host Host to connect to. This may be a hostname to resolve, an
 *  IPv4 address, or an IPv6 address. This is passed directly to getaddrinfo().
 * @param[in] port Remote port to connect to.
 * @param[in] user User to use while establishing the connection.
 * @param[in] password Password to use while establishing the connection.
 * @param[in] db Initial database to connect to.
 * @param[in] options Drizzle connection options to add.
 * @return Same return as drizzle_con_create().
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_add_tcp(drizzle_st *drizzle,
                                    const char *host, in_port_t port,
                                    const char *user, const char *password,
                                    const char *db,
                                    drizzle_con_options_t options);

/**
 * Add unix domain socket connection with common arguments.
 *
 * @param[in] drizzle Drizzle structure previously initialized with
 *  drizzle_create() or drizzle_clone().
 * @param[in] con Caller allocated structure, or NULL to allocate one.
 * @param[in] uds Path to unix domain socket to use for connection.
 * @param[in] user User to use while establishing the connection.
 * @param[in] password Password to use while establishing the connection.
 * @param[in] db Initial database to connect to.
 * @param[in] options Drizzle connection options to add.
 * @return Same return as drizzle_con_create().
 */
DRIZZLE_API
drizzle_con_st *drizzle_con_add_uds(drizzle_st *drizzle,
                                    const char *uds, const char *user,
                                    const char *password, const char *db,
                                    drizzle_con_options_t options);

/** @} */

#ifdef  __cplusplus
}
#endif
