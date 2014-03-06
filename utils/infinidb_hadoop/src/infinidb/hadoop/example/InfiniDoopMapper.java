/*
 * Copyright (c) 2014 InfiniDB, Inc.
 *
 * InfiniDB, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

