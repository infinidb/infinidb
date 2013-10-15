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
 * @brief Command definitions
 */

#include <libdrizzle-2.0/common.h>

/*
 * Private variables.
 */

static drizzle_command_drizzle_t _command_drizzle_map[]=
{
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_QUIT,
 DRIZZLE_COMMAND_DRIZZLE_INIT_DB,
 DRIZZLE_COMMAND_DRIZZLE_QUERY,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_SHUTDOWN,
 DRIZZLE_COMMAND_DRIZZLE_END, // STATISTICS
 DRIZZLE_COMMAND_DRIZZLE_END, // PROCESS_INFO
 DRIZZLE_COMMAND_DRIZZLE_END, // CONNECT
 DRIZZLE_COMMAND_DRIZZLE_KILL,// PROCESS_KILL
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_PING,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END,
 DRIZZLE_COMMAND_DRIZZLE_END
};

/*
 * State Definitions
 */

drizzle_return_t drizzle_state_command_read(drizzle_con_st *con)
{
  drizzle_log_debug(con->drizzle, "drizzle_state_command_read");

  if (con->buffer_size == 0)
  {
    drizzle_state_push(con, drizzle_state_read);
    return DRIZZLE_RETURN_OK;
  }

  if (con->command_total == 0)
  {
    con->command= (drizzle_command_t)(con->buffer_ptr[0]);
    con->buffer_ptr++;
    con->buffer_size--;

    con->command_total= (con->packet_size - 1);
  }

  if (con->buffer_size < (con->command_total - con->command_offset))
  {
    con->command_size= con->buffer_size;
    con->command_offset+= con->command_size;
  }
  else
  {
    con->command_size= (con->command_total - con->command_offset);
    con->command_offset= con->command_total;
  }

  con->command_data= con->buffer_ptr;
  con->buffer_ptr+= con->command_size;
  con->buffer_size-= con->command_size;

  if (con->command_offset == con->command_total)
  {
    drizzle_state_pop(con);
  }
  else
  {
    return DRIZZLE_RETURN_PAUSE;
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_command_write(drizzle_con_st *con)
{
  uint8_t *start;
  uint8_t *ptr;
  size_t free_size;
  drizzle_return_t ret;

  drizzle_log_debug(con->drizzle, "drizzle_state_command_write");

  if (con->command_data == NULL && con->command_total != 0 &&
      con->command != DRIZZLE_COMMAND_CHANGE_USER)
  {
    return DRIZZLE_RETURN_PAUSE;
  }

  if (con->buffer_size == 0)
  {
    con->buffer_ptr= con->buffer;
    start= con->buffer;
  }
  else
    start= con->buffer_ptr + con->buffer_size;

  if (con->command_offset == 0)
  {
    /* Make sure we can fit the largest non-streaming packet, currently a
       DRIZZLE_COMMAND_CHANGE_USER command. */

    con->packet_size= 1  /* Command */
                    + strlen(con->user) + 1
                    + 1  /* Scramble size */
                    + DRIZZLE_MAX_SCRAMBLE_SIZE
                    + strlen(con->schema) +1;

    /* Flush buffer if there is not enough room. */
    free_size= (size_t)DRIZZLE_MAX_BUFFER_SIZE - (size_t)(start - con->buffer);
    if (free_size < con->packet_size)
    {
      drizzle_state_push(con, drizzle_state_write);
      return DRIZZLE_RETURN_OK;
    }

    /* Store packet size at the end since it may change. */
    con->packet_number= 1;
    ptr= start;
    ptr[3]= 0;
    if (con->options & DRIZZLE_CON_MYSQL)
    {
      ptr[4]= uint8_t(con->command);
    }
    else
    {
      ptr[4]= uint8_t(_command_drizzle_map[con->command]);
    }
    ptr+= 5;

    if (con->command == DRIZZLE_COMMAND_CHANGE_USER)
    {
      ptr= drizzle_pack_auth(con, ptr, &ret);
      if (ret != DRIZZLE_RETURN_OK)
        return ret;

      con->buffer_size+= (4 + con->packet_size);
    }
    else if (con->command_total == 0)
    {
      con->packet_size= 1;
      con->buffer_size+= 5;
    }
    else
    {
      con->packet_size= 1 + con->command_total;
      free_size-= 5;

      /* Copy as much of the data in as we can into the write buffer. */
      if (con->command_size <= free_size)
      {
        memcpy(ptr, con->command_data, con->command_size);
        con->command_offset= con->command_size;
        con->command_data= NULL;
        con->buffer_size+= 5 + con->command_size;
      }
      else
      {
        memcpy(ptr, con->command_data, free_size);
        con->command_offset= free_size;
        con->command_data+= free_size;
        con->command_size-= free_size;
        con->buffer_size+= 5 + free_size;
      }
    }

    /* Store packet size now. */
    drizzle_set_byte3(start, con->packet_size);
  }
  else
  {
    /* Write directly from the caller buffer for the rest. */
    con->buffer_ptr= con->command_data;
    con->buffer_size= con->command_size;
    con->command_offset+= con->command_size;
    con->command_data= NULL;
  }

  if (con->command_offset == con->command_total)
  {
    drizzle_state_pop(con);

    if (!(con->options & (DRIZZLE_CON_RAW_PACKET |
                          DRIZZLE_CON_NO_RESULT_READ)) &&
        con->command != DRIZZLE_COMMAND_FIELD_LIST)
    {
      drizzle_state_push(con, drizzle_state_result_read);
      drizzle_state_push(con, drizzle_state_packet_read);
    }
  }

  drizzle_state_push(con, drizzle_state_write);

  return DRIZZLE_RETURN_OK;
}
