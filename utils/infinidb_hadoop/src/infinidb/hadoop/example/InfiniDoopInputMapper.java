/* Copyright (C) 2014 InfiniDB, Inc.

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InfiniDoopInputMapper extends MapReduceBase implements 
 Mapper<LongWritable, InfiniDoopRecord, LongWritable, Text> {

	public void map(LongWritable key, InfiniDoopRecord val,
			OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		output.collect(new LongWritable(val.id), new Text(val.name));
	}

}
