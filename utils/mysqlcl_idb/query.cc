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
 * @brief Query definitions
 */

#include <libdrizzle-2.0/common.h>

drizzle_result_st *drizzle_query(drizzle_con_st *con, drizzle_result_st *result,
                                 const char *query, size_t size,
                                 drizzle_return_t *ret_ptr)
{
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_QUERY,
                                   (uint8_t *)query, size, size, ret_ptr);
}

drizzle_result_st *drizzle_query_str(drizzle_con_st *con,
                                     drizzle_result_st *result,
                                     const char *query, 
                                     drizzle_return_t *ret_ptr)
{
  size_t size= strlen(query);

  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_QUERY, (uint8_t *)query, size, size, ret_ptr);
}

drizzle_result_st *drizzle_query_inc(drizzle_con_st *con,
                                     drizzle_result_st *result,
                                     const char *query, size_t size,
                                     size_t total, drizzle_return_t *ret_ptr)
{
  return drizzle_con_command_write(con, result, DRIZZLE_COMMAND_QUERY, (uint8_t *)query, size, total, ret_ptr);
}

drizzle_query_st *drizzle_query_add(drizzle_st *drizzle,
                                    drizzle_query_st *query,
                                    drizzle_con_st *con,
                                    drizzle_result_st *result,
                                    const char *query_string, size_t size,
                                    drizzle_query_options_t,
                                    void *context)
{
  // @note drizzle_query_st handle the null drizzle case
  query= drizzle_query_create(drizzle, query);
  if (query == NULL)
  {
    return NULL;
  }

  drizzle_query_set_con(query, con);
  drizzle_query_set_result(query, result);
  drizzle_query_set_string(query, query_string, size);
  drizzle_query_set_context(query, context);

  return query;
}

drizzle_query_st *drizzle_query_create(drizzle_st *drizzle, drizzle_query_st *query)
{
  if (drizzle == NULL)
  {
    return NULL;
  }

  if (query == NULL)
  {
    query= new (std::nothrow) drizzle_query_st;

    if (query == NULL)
    {
      return NULL;
    }
    query->options.is_allocated= true;
  }
  else
  {
    query->prev= NULL;
    query->state= DRIZZLE_QUERY_STATE_INIT;
    query->con= NULL;
    query->result= NULL;
    query->string= NULL;
    query->size= 0;
    query->context= NULL;
    query->context_free_fn= NULL;
    query->options.is_allocated= false;
  }

  query->drizzle= drizzle;

  if (drizzle->query_list)
  {
    drizzle->query_list->prev= query;
  }
  query->next= drizzle->query_list;
  drizzle->query_list= query;
  drizzle->query_count++;
  drizzle->query_new++;

  return query;
}

void drizzle_query_free(drizzle_query_st *query)
{
  if (query == NULL)
  {
    return;
  }

  if (query->context != NULL && query->context_free_fn != NULL)
  {
    query->context_free_fn(query, query->context);
  }

  if (query->drizzle->query_list == query)
  {
    query->drizzle->query_list= query->next;
  }

  if (query->prev)
  {
    query->prev->next= query->next;
  }

  if (query->next)
  {
    query->next->prev= query->prev;
  }

  query->drizzle->query_count--;

  if (query->options.is_allocated)
  {
    delete query;
  }
}

void drizzle_query_free_all(drizzle_st *drizzle)
{
  while (drizzle->query_list != NULL)
  {
    drizzle_query_free(drizzle->query_list);
  }
}

drizzle_con_st *drizzle_query_con(drizzle_query_st *query)
{
  if (query == NULL)
  {
    return NULL;
  }

  return query->con;
}

void drizzle_query_set_con(drizzle_query_st *query, drizzle_con_st *con)
{
  if (query == NULL)
  {
    return;
  }

  query->con= con;
}

drizzle_result_st *drizzle_query_result(drizzle_query_st *query)
{
  if (query == NULL)
  {
    return NULL;
  }

  return query->result;
}

void drizzle_query_set_result(drizzle_query_st *query,
                              drizzle_result_st *result)
{
  if (query == NULL)
  {
    return;
  }

  query->result= result;
}

char *drizzle_query_string(drizzle_query_st *query, size_t *size)
{
  if (query == NULL)
  {
    return NULL;
  }

  *size= query->size;
  return (char *)(query->string);
}

void drizzle_query_set_string(drizzle_query_st *query, const char *string,
                              size_t size)
{
  if (query == NULL)
  {
    return;
  }

  query->string= string;
  query->size= size;
}

int drizzle_query_options(drizzle_query_st *)
{
  return 0;
}

void drizzle_query_set_options(drizzle_query_st *, int)
{
}

void drizzle_query_add_options(drizzle_query_st *, int)
{
}

void drizzle_query_remove_options(drizzle_query_st *, int)
{
}

void *drizzle_query_context(drizzle_query_st *query)
{
  if (query == NULL)
  {
    return NULL;
  }

  return query->context;
}

void drizzle_query_set_context(drizzle_query_st *query, void *context)
{
  if (query == NULL)
  {
    return;
  }

  query->context= context;
}

void drizzle_query_set_context_free_fn(drizzle_query_st *query,
                                       drizzle_query_context_free_fn *function)
{
  if (query == NULL)
  {
    return;
  }

  query->context_free_fn= function;
}

static void drizzle_query_run_state(drizzle_query_st* query,
                                    drizzle_return_t* ret_ptr)
{
  if (query == NULL)
  {
    return;
  }

  switch (query->state)
  {
  case DRIZZLE_QUERY_STATE_INIT:
    query->state= DRIZZLE_QUERY_STATE_QUERY;

  case DRIZZLE_QUERY_STATE_QUERY:
    query->result= drizzle_query(query->con, query->result, query->string,
                                 query->size, ret_ptr);
    if (*ret_ptr == DRIZZLE_RETURN_IO_WAIT)
    {
      return;
    }
    else if (*ret_ptr != DRIZZLE_RETURN_OK)
    {
      query->state= DRIZZLE_QUERY_STATE_DONE;
      return;
    }

    query->state= DRIZZLE_QUERY_STATE_RESULT;

  case DRIZZLE_QUERY_STATE_RESULT:
    *ret_ptr= drizzle_result_buffer(query->result);
    if (*ret_ptr == DRIZZLE_RETURN_IO_WAIT)
    {
      return;
    }

    query->state= DRIZZLE_QUERY_STATE_DONE;
    return;

  default:
  case DRIZZLE_QUERY_STATE_DONE:
    return;
  }
}

drizzle_query_st *drizzle_query_run(drizzle_st *drizzle,
                                    drizzle_return_t *ret_ptr)
{
  drizzle_return_t unused;
  if (ret_ptr == NULL)
  {
    ret_ptr= &unused;
  }

  if (drizzle == NULL)
  {
    *ret_ptr= DRIZZLE_RETURN_INVALID_ARGUMENT;
    return NULL;
  }

  if (drizzle->query_new == 0 && drizzle->query_running == 0)
  {
    *ret_ptr= DRIZZLE_RETURN_OK;
    return NULL;
  }

  drizzle_st::options_t options= drizzle->options;
  drizzle->options.is_non_blocking= false;

  /* Check to see if any queries need to be started. */
  if (drizzle->query_new > 0)
  {
    for (drizzle_query_st *query= drizzle->query_list; query != NULL; query= query->next)
    {
      if (query->state != DRIZZLE_QUERY_STATE_INIT)
      {
        continue;
      }

      drizzle->query_new--;
      drizzle->query_running++;
      assert(query->con->query == NULL);
      query->con->query= query;

      drizzle_query_run_state(query, ret_ptr);
      if (*ret_ptr != DRIZZLE_RETURN_IO_WAIT)
      {
        assert(query->state == DRIZZLE_QUERY_STATE_DONE);
        drizzle->query_running--;
        drizzle->options= options;
        query->con->query= NULL;
        if (*ret_ptr == DRIZZLE_RETURN_ERROR_CODE || *ret_ptr == DRIZZLE_RETURN_OK)
        {
          return query;
        }
        return NULL;
      }
    }
    assert(drizzle->query_new == 0);
  }

  while (1)
  {
    drizzle_con_st *con;

    /* Loop through each active connection. */
    while ((con= drizzle_con_ready(drizzle)) != NULL)
    {
      drizzle_query_st *query= con->query;
      drizzle_query_run_state(query, ret_ptr);
      if (query->state == DRIZZLE_QUERY_STATE_DONE)
      {
        drizzle->query_running--;
        drizzle->options= options;
        con->query= NULL;
        return query;
      }
      assert(*ret_ptr == DRIZZLE_RETURN_IO_WAIT);
    }

    if (options.is_non_blocking)
    {
      *ret_ptr= DRIZZLE_RETURN_IO_WAIT;
      return NULL;
    }

    *ret_ptr= drizzle_con_wait(drizzle);
    if (*ret_ptr != DRIZZLE_RETURN_OK)
    {
      drizzle->options= options;
      return NULL;
    }
  }
}

drizzle_return_t drizzle_query_run_all(drizzle_st *drizzle)
{
  if (drizzle == NULL)
  {
    return DRIZZLE_RETURN_INVALID_ARGUMENT;
  }

  while (drizzle->query_new > 0 || drizzle->query_running > 0)
  {
    drizzle_return_t ret;

    (void)drizzle_query_run(drizzle, &ret);
    if (ret != DRIZZLE_RETURN_OK && ret != DRIZZLE_RETURN_ERROR_CODE)
    {
      return ret;
    }
  }

  return DRIZZLE_RETURN_OK;
}

ssize_t drizzle_escape_string(char *to, size_t max_to_size, const char *from, size_t from_size)
{
  ssize_t to_size= 0;
  char newchar;
  const char *end;

  for (end= from + from_size; from < end; from++)
  {
    newchar= 0;
    /* All multi-byte UTF8 characters have the high bit set for all bytes. */
    if (!(*from & 0x80))
    {
      switch (*from)
      {
      case 0:
        newchar= '0';
        break;
      case '\n':
        newchar= 'n';
        break;
      case '\r':
        newchar= 'r';
        break;
      case '\032':
        newchar= 'Z';
        break;
      case '\\':
        newchar= '\\';
        break;
      case '\'':
        newchar= '\'';
        break;
      case '"':
        newchar= '"';
        break;
      default:
        break;
      }
    }
    if (newchar != '\0')
    {
      if ((size_t)to_size + 2 > max_to_size)
      {
        return -1;
      }

      *to++= '\\';
      *to++= newchar;
      to_size++;
    }
    else
    {
      if ((size_t)to_size + 1 > max_to_size)
      {
        return -1;
      }

      *to++= *from;
    }
    to_size++;
  }

  *to= 0;

  return to_size;
}

size_t drizzle_hex_string(char *to, const char *from, size_t from_size)
{
  static const char hex_map[]= "0123456789ABCDEF";
  const char *from_end;

  for (from_end= from + from_size; from != from_end; from++)
  {
    *to++= hex_map[((unsigned char) *from) >> 4];
    *to++= hex_map[((unsigned char) *from) & 0xF];
  }

  *to= 0;

  return from_size * 2;
}

void drizzle_mysql_password_hash(char *to, const char *from, size_t from_size)
{
  SHA1_CTX ctx;
  uint8_t hash_tmp1[SHA1_DIGEST_LENGTH];
  uint8_t hash_tmp2[SHA1_DIGEST_LENGTH];

  SHA1Init(&ctx);
  SHA1Update(&ctx, (const uint8_t*)from, from_size);
  SHA1Final(hash_tmp1, &ctx);

  SHA1Init(&ctx);
  SHA1Update(&ctx, hash_tmp1, SHA1_DIGEST_LENGTH);
  SHA1Final(hash_tmp2, &ctx);

  (void)drizzle_hex_string(to, (char*)hash_tmp2, SHA1_DIGEST_LENGTH);
}
