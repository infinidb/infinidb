/* Copyright (C) 2013 Calpont Corp.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id$

package infinidb.hadoop.example;

import java.io.IOException;
import java.io.*;
import java.sql.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** Dummy mapper, basically doing nothing. the real job is invoked by input format */
public class InfiniDoopMapper extends MapReduceBase implements 
 Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

	public void map(NullWritable key, NullWritable val,
			OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
		NullWritable n = NullWritable.get();
		output.collect(n, n);
	}
}

