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
 * @brief Field definitions
 */

#include <libdrizzle-2.0/common.h>

/*
 * Client definitions
 */

drizzle_field_t drizzle_field_read(drizzle_result_st *result, size_t *offset,
                                   size_t *size, size_t *total,
                                   drizzle_return_t *ret_ptr)
{
  if (drizzle_state_none(result->con))
  {
    if (result->field_current == result->column_count)
    {
      *ret_ptr= DRIZZLE_RETURN_ROW_END;
      return NULL;
    }

    drizzle_state_push(result->con, drizzle_state_field_read);
  }

  *ret_ptr= drizzle_state_loop(result->con);
  if (*ret_ptr == DRIZZLE_RETURN_OK &&
      result->options & DRIZZLE_RESULT_ROW_BREAK)
  {
    *ret_ptr= DRIZZLE_RETURN_ROW_BREAK;
  }

  *offset= result->field_offset;
  *size= result->field_size;
  *total= result->field_total;

  return result->field;
}

drizzle_field_t drizzle_field_buffer(drizzle_result_st *result, size_t *total,
                                     drizzle_return_t *ret_ptr)
{
  size_t offset= 0;
  size_t size= 0;

  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  drizzle_field_t field= drizzle_field_read(result, &offset, &size, total, ret_ptr);
  if (*ret_ptr != DRIZZLE_RETURN_OK)
  {
    return NULL;
  }

  if (field == NULL)
  {
    *total= 0;
    return NULL;
  }

  if (result->field_buffer == NULL)
  {
    result->field_buffer= new (std::nothrow) drizzle_field_t_type[(*total) + 1];

    if (result->field_buffer == NULL)
    {
      *ret_ptr= DRIZZLE_RETURN_MEMORY;
      *total= 0;
      return NULL;
    }
  }

  memcpy(result->field_buffer + offset, field, size);

  while ((offset + size) != (*total))
  {
    field= drizzle_field_read(result, &offset, &size, total, ret_ptr);
    if (*ret_ptr != DRIZZLE_RETURN_OK)
    {
      return NULL;
    }

    memcpy(result->field_buffer + offset, field, size);
  }

  field= result->field_buffer;
  result->field_buffer= NULL;
  field[*total]= 0;

  return field;
}

void drizzle_field_free(drizzle_field_t field)
{
  delete[] field;
}

/*
 * Server definitions
 */

drizzle_return_t drizzle_field_write(drizzle_result_st *result,
                                     const drizzle_field_t field, size_t size,
                                     size_t total)
{
  drizzle_return_t ret;

  if (drizzle_state_none(result->con))
  {
    if (result->options & DRIZZLE_RESULT_ROW_BREAK)
    {
      result->options&= ~DRIZZLE_RESULT_ROW_BREAK;
      result->field= field;
      result->field_size= size;
    }
    else
    {
      result->field= field;
      result->field_size= size;
      result->field_offset= 0;
      result->field_total= total;
    }

    drizzle_state_push(result->con, drizzle_state_field_write);
  }
  else if (result->field == NULL)
  {
    result->field= field;
    result->field_size= size;
  }

  ret= drizzle_state_loop(result->con);
  if (ret == DRIZZLE_RETURN_PAUSE)
    ret= DRIZZLE_RETURN_OK;

  return ret;
}

/*
 * Internal state functions.
 */

drizzle_return_t drizzle_state_field_read(drizzle_con_st *con)
{
  drizzle_return_t ret;

  drizzle_log_debug(con->drizzle, "drizzle_state_field_read");

  if (con->buffer_size == 0)
  {
    drizzle_state_push(con, drizzle_state_read);
    return DRIZZLE_RETURN_OK;
  }

  con->result->field_offset+= con->result->field_size;
  if (con->result->field_offset == con->result->field_total)
  {
    con->result->field_offset= 0;
    con->result->field_size= 0;

    con->result->field_total= (size_t)drizzle_unpack_length(con, &ret);
    if (ret == DRIZZLE_RETURN_NULL_SIZE)
    {
      con->result->field= NULL;
      con->result->field_current++;
      drizzle_state_pop(con);
      return DRIZZLE_RETURN_OK;
    }
    else if (ret != DRIZZLE_RETURN_OK)
    {
      if (ret == DRIZZLE_RETURN_IO_WAIT)
      {
        drizzle_state_push(con, drizzle_state_read);
        return DRIZZLE_RETURN_OK;
      }

      return ret;
    }

    drizzle_log_debug(con->drizzle,
                      "field_offset= %zu, field_size= %zu, field_total= %zu",
                      con->result->field_offset, con->result->field_size,
                      con->result->field_total);

    if ((size_t)(con->buffer_size) >= con->result->field_total)
      con->result->field_size= con->result->field_total;
    else
      con->result->field_size= con->buffer_size;
  }
  else
  {
    if ((con->result->field_offset + con->buffer_size) >=
        con->result->field_total)
    {
      con->result->field_size= (con->result->field_total -
                                con->result->field_offset);
    }
    else
      con->result->field_size= con->buffer_size;
  }

  /* This is a special case when a row is larger than the packet size. */
  if (con->result->field_size > (size_t)con->packet_size)
  {
    con->result->field_size= con->packet_size;

    if (con->options & DRIZZLE_CON_RAW_PACKET)
      con->result->options|= DRIZZLE_RESULT_ROW_BREAK;
    else
    {
      drizzle_state_pop(con);
      drizzle_state_push(con, drizzle_state_packet_read);
      drizzle_state_push(con, drizzle_state_field_read);
    }
  }

  con->result->field= (char *)con->buffer_ptr;
  con->buffer_ptr+= con->result->field_size;
  con->buffer_size-= con->result->field_size;
  con->packet_size-= con->result->field_size;

  drizzle_log_debug(con->drizzle,
                    "field_offset= %zu, field_size= %zu, field_total= %zu",
                    con->result->field_offset, con->result->field_size,
                    con->result->field_total);

  if ((con->result->field_offset + con->result->field_size) ==
      con->result->field_total)
  {
    if (con->result->column_buffer != NULL &&
        con->result->column_buffer[con->result->field_current].max_size <
        con->result->field_total)
    {
      con->result->column_buffer[con->result->field_current].max_size=
                                                       con->result->field_total;
    }

    con->result->field_current++;
  }

  if (con->result->field_total == 0 || con->result->field_size > 0 ||
      con->packet_size == 0)
  {
    drizzle_state_pop(con);
  }

  return DRIZZLE_RETURN_OK;
}

drizzle_return_t drizzle_state_field_write(drizzle_con_st *con)
{
  uint8_t *start= con->buffer_ptr + con->buffer_size;
  uint8_t *ptr;
  size_t free_size;
  drizzle_result_st *result= con->result;

  drizzle_log_debug(con->drizzle, "drizzle_state_field_write");

  if (result->field == NULL && result->field_total != 0)
    return DRIZZLE_RETURN_PAUSE;

  free_size= (size_t)DRIZZLE_MAX_BUFFER_SIZE - (size_t)(start - con->buffer);
  ptr= start;

  if (result->field_offset == 0)
  {
    /* Make sure we can fit the max length and 1 byte of data in (9 + 1). */
    if (free_size < 10)
    {
      drizzle_state_push(con, drizzle_state_write);
      return DRIZZLE_RETURN_OK;
    }

    if (result->field == NULL)
    {
      ptr[0]= 251;
      ptr++;
    }
    else if (result->field_total == 0)
    {
      ptr[0]= 0;
      ptr++;
    }
    else
      ptr= drizzle_pack_length(result->field_total, ptr);

    free_size-= (size_t)(ptr - start);
    con->buffer_size+= (size_t)(ptr - start);
    con->packet_size-= (size_t)(ptr - start);
  }
  else if (result->field_size > DRIZZLE_BUFFER_COPY_THRESHOLD)
  {
    /* Flush the internal buffer first. */
    if (con->buffer_size != 0)
    {
      drizzle_state_push(con, drizzle_state_write);
      return DRIZZLE_RETURN_OK;
    }

    /* We do this to write directly from the field buffer to avoid memcpy(). */
    con->buffer_ptr= (uint8_t *)result->field;
    con->buffer_size= result->field_size;
    con->packet_size-= result->field_size;
    result->field_offset+= result->field_size;
    result->field= NULL;

    if (result->field_offset == result->field_total)
      drizzle_state_pop(con);
    else if (con->packet_size == 0)
    {
      con->result->options|= DRIZZLE_RESULT_ROW_BREAK;
      drizzle_state_pop(con);
    }

    drizzle_state_push(con, drizzle_state_write);
    return DRIZZLE_RETURN_OK;
  }

  if (result->field_size == 0)
    drizzle_state_pop(con);
  else
  {
    if (result->field_size < free_size)
      free_size= result->field_size;

    memcpy(ptr, result->field, free_size);
    result->field_offset+= free_size;
    con->buffer_size+= free_size;
    con->packet_size-= free_size;

    if (result->field_offset == result->field_total)
    {
      result->field= NULL;
      drizzle_state_pop(con);
    }
    else
    {
      if (con->packet_size == 0)
      {
        con->result->options|= DRIZZLE_RESULT_ROW_BREAK;
        drizzle_state_pop(con);
      }

      if (result->field_size == free_size)
        result->field= NULL;
      else
      {
        result->field+= free_size;
        result->field_size-= free_size;
        drizzle_state_push(con, drizzle_state_write);
      }
    }
  }

  return DRIZZLE_RETURN_OK;
}
