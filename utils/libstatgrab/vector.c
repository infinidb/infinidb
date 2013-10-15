/*
 * i-scream libstatgrab
 * http://www.i-scream.org
 * Copyright (C) 2000-2004 i-scream
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 *
 * $Id: vector.c,v 1.9 2004/04/07 14:53:40 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>

#include "tools.h"
#include "vector.h"

void *sg_vector_resize(void *vector, vector_header *h, int count) {
	int new_count, i;

	/* Destroy any now-unused items.
	 *
	 * Note that there's an assumption here that making the vector smaller
	 * will never fail; if it did, then we would have destroyed items here
	 * but not actually got rid of the vector pointing to them before the
	 * error return.) */
	if (count < h->used_count && h->destroy_fn != NULL) {
		for (i = count; i < h->used_count; i++) {
			h->destroy_fn((void *) (vector + i * h->item_size));
		}
	}

	/* Round up the desired size to the next multiple of the block size. */
	new_count =  ((count - 1 + h->block_size) / h->block_size)
		     * h->block_size;

	/* Resize the vector if necessary. */
	if (new_count != h->alloc_count) {
		char *new_vector;

		new_vector = sg_realloc(vector, new_count * h->item_size);
		if (new_vector == NULL && new_count != 0) {
			/* Out of memory -- free the contents of the vector. */
			sg_vector_resize(vector, h, 0);
			h->error = -1;
			return vector;
		}

		vector = new_vector;
		h->alloc_count = new_count;
	}

	/* Initialise any new items. */
	if (count > h->used_count && h->init_fn != NULL) {
		for (i = h->used_count; i < count; i++) {
			h->init_fn((void *) (vector + i * h->item_size));
		}
	}

	h->used_count = count;
	h->error = 0;
	return vector;
}

