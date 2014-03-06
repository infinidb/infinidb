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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InfiniDoopRecord implements Writable, DBWritable, WritableComparable<InfiniDoopRecord> {
	  long id;
	  String name;
	  
	  public void readFields(DataInput in) throws IOException {
	    this.id = in.readLong();
	    this.name = Text.readString(in);
	  }

	  public void readFields(ResultSet resultSet)
	      throws SQLException {
	    this.id = resultSet.getLong(1);
	    this.name = resultSet.getString(2);
	  }

	  public void write(DataOutput out) throws IOException {
	    out.writeLong(this.id);
	    Text.writeString(out, this.name);
	  }

	  public void write(PreparedStatement stmt) throws SQLException {
	    stmt.setLong(1, this.id);
	    stmt.setString(2, this.name);
	  }
	  
	  public int compareTo(InfiniDoopRecord w) {
		  return (this.id < w.id ? -1 :(this.id == w.id ? 0 : 1));
	  }
	}
