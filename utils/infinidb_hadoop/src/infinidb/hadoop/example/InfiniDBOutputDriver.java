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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.Formatter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.db.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.*;

import infinidb.hadoop.db.*;

public class InfiniDBOutputDriver extends Configured implements Tool
{
	public int run (String[] args) throws Exception
	{	
		Configuration conf = new Configuration();
		JobConf jobconf = new JobConf(conf, InfiniDoopDriver.class);
		DBConfiguration.configureDB(jobconf,
		 		    "com.mysql.jdbc.Driver",
		 		    "jdbc:mysql://srvswint4/tpch1","root", "");
		String [] fields = { "n_nationkey", "n_name" };
		String [] outFields = {"id", "name"};
		jobconf.setInputFormat(IDBFileInputFormat.class);
		jobconf.setOutputFormat(InfiniDBOutputFormat.class);
		jobconf.setOutputKeyClass(NullWritable.class);
		jobconf.setOutputValueClass(Text.class);
		InfiniDBOutputFormat.setOutput(jobconf, "db", outFields);
		InfiniDBConfiguration idbconf = new InfiniDBConfiguration(jobconf);
		idbconf.setInputPath("input");
		idbconf.setOutputPath("output");
		idbconf.setInfiniDBHome("/usr/local/Calpont");

		jobconf.setMapperClass(InfiniDoopMapper.class);
		jobconf.setNumMapTasks(1);
		jobconf.setNumReduceTasks(2);
		JobClient client = new JobClient();
		client.setConf(jobconf);
		try {
			JobClient.runJob(jobconf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0;
}

public static void main(String [] args) throws Exception
{
	int ret = ToolRunner.run(new InfiniDBOutputDriver(), args);
	System.exit(ret);
}

}
