/*
 * Drizzle Client & Protocol Library
 *
 * Copyright (C) 2011 Brian Aker (brian@tangent.org)
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
 * @ingroup drizzle_command 
 * Commands for drizzle_command functions.
 */
enum drizzle_command_t
{
  DRIZZLE_COMMAND_SLEEP,               /* Not used currently. */
  DRIZZLE_COMMAND_QUIT,
  DRIZZLE_COMMAND_INIT_DB,
  DRIZZLE_COMMAND_QUERY,
  DRIZZLE_COMMAND_FIELD_LIST,          /* Deprecated. */
  DRIZZLE_COMMAND_CREATE_DB,           /* Deprecated. */
  DRIZZLE_COMMAND_DROP_DB,             /* Deprecated. */
  DRIZZLE_COMMAND_REFRESH,
  DRIZZLE_COMMAND_SHUTDOWN,
  DRIZZLE_COMMAND_STATISTICS,
  DRIZZLE_COMMAND_PROCESS_INFO,        /* Deprecated. */
  DRIZZLE_COMMAND_CONNECT,             /* Not used currently. */
  DRIZZLE_COMMAND_PROCESS_KILL,        /* Deprecated. */
  DRIZZLE_COMMAND_DEBUG,
  DRIZZLE_COMMAND_PING,
  DRIZZLE_COMMAND_TIME,                /* Not used currently. */
  DRIZZLE_COMMAND_DELAYED_INSERT,      /* Not used currently. */
  DRIZZLE_COMMAND_CHANGE_USER,
  DRIZZLE_COMMAND_BINLOG_DUMP,         /* Not used currently. */
  DRIZZLE_COMMAND_TABLE_DUMP,          /* Not used currently. */
  DRIZZLE_COMMAND_CONNECT_OUT,         /* Not used currently. */
  DRIZZLE_COMMAND_REGISTER_SLAVE,      /* Not used currently. */
  DRIZZLE_COMMAND_STMT_PREPARE,        /* Not used currently. */
  DRIZZLE_COMMAND_STMT_EXECUTE,        /* Not used currently. */
  DRIZZLE_COMMAND_STMT_SEND_LONG_DATA, /* Not used currently. */
  DRIZZLE_COMMAND_STMT_CLOSE,          /* Not used currently. */
  DRIZZLE_COMMAND_STMT_RESET,          /* Not used currently. */
  DRIZZLE_COMMAND_SET_OPTION,          /* Not used currently. */
  DRIZZLE_COMMAND_STMT_FETCH,          /* Not used currently. */
  DRIZZLE_COMMAND_DAEMON,              /* Not used currently. */
  DRIZZLE_COMMAND_END                  /* Not used currently. */
};

#ifndef __cplusplus
typedef enum drizzle_command_t drizzle_command_t;
#endif

/**
 * @ingroup drizzle_command 
 * Commands for the Drizzle protocol functions.
 */
enum drizzle_command_drizzle_t
{
  DRIZZLE_COMMAND_DRIZZLE_SLEEP,
  DRIZZLE_COMMAND_DRIZZLE_QUIT,
  DRIZZLE_COMMAND_DRIZZLE_INIT_DB,
  DRIZZLE_COMMAND_DRIZZLE_QUERY,
  DRIZZLE_COMMAND_DRIZZLE_SHUTDOWN,
  DRIZZLE_COMMAND_DRIZZLE_CONNECT,
  DRIZZLE_COMMAND_DRIZZLE_PING,
  DRIZZLE_COMMAND_DRIZZLE_KILL,
  DRIZZLE_COMMAND_DRIZZLE_END
};

#ifndef __cplusplus
typedef enum drizzle_command_drizzle_t drizzle_command_drizzle_t;
#endif

