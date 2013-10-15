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
