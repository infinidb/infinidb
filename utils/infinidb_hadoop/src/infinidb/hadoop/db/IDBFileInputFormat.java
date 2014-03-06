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

package infinidb.hadoop.db;

import infinidb.hadoop.db.InfiniDBConfiguration;

import java.io.*;
import java.sql.*;
import java.util.Date;
import java.util.Formatter;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IDBFileInputFormat extends org.apache.hadoop.mapred.FileInputFormat<NullWritable, NullWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(DBInputFormat.class);
	
	@Override
	public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit arg0, JobConf arg1,
	      Reporter arg2) throws IOException 
	{
		final String filename = ((FileSplit)arg0).getPath().toString();
		final JobConf job = arg1;
		
		return new RecordReader<NullWritable, NullWritable>() 
		{
			private boolean unread = true;

			@Override
			public void close() throws IOException 
			{}

			@Override
			public NullWritable createKey()
			{
				return NullWritable.get();
			}

			@Override
			public NullWritable createValue()
			{
				return NullWritable.get();
			}

			@Override
			public long getPos() throws IOException
			{
				return 0;
			}

			@Override
			public float getProgress() throws IOException
			{
				return unread ? 0 : 1;
			}

			@Override
			/* spawn a cpimport process for each input file */
			public boolean next(NullWritable arg0, NullWritable arg1) throws IOException 
			{
				InfiniDBConfiguration dbConf = new InfiniDBConfiguration(job);
				String schemaName = dbConf.getOutputSchemaName(); 
				String tableName = (filename.substring(filename.lastIndexOf('/')+1, filename.length()));
				tableName = tableName.substring(0, tableName.lastIndexOf('.'));
				String output = job.get("mapred.output.dir");
				if (unread)
				{
					try
					{
						StringBuilder loadCmdStr = new StringBuilder();
						loadCmdStr.append(dbConf.getInfiniDBHome());
						loadCmdStr.append("/bin/");
						loadCmdStr.append("infinidoop_load.sh ");
						loadCmdStr.append(filename);
						loadCmdStr.append(" ");
						loadCmdStr.append(schemaName);
						loadCmdStr.append(" ");
						loadCmdStr.append(tableName);

						Process lChldProc = Runtime.getRuntime().exec(loadCmdStr.toString());
						 
						// Wait for the child to exit
						lChldProc.waitFor();
						BufferedReader lChldProcOutStream = new BufferedReader(new InputStreamReader(lChldProc.getInputStream()));
						BufferedReader stdError = new BufferedReader(new InputStreamReader(lChldProc.getErrorStream()));
						
						String lChldProcOutPutStr = null;
						StringBuffer outpath = new StringBuffer();
						outpath.append(job.getWorkingDirectory());
						outpath.append("/");
						outpath.append(output);
						outpath.append("/");
						outpath.append(tableName);
						outpath.append(".log");

						Path pt=new Path(outpath.toString());
						FileSystem fs = FileSystem.get(new Configuration());
						BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, false)));

						// catch output
						while ((lChldProcOutPutStr = lChldProcOutStream.readLine()) != null)
						{
							br.write(lChldProcOutPutStr);
							br.newLine();
						}
						
						// catch error
						while ((lChldProcOutPutStr = stdError.readLine()) != null)
						{
							br.write(lChldProcOutPutStr);
							br.newLine();
						}

						//br.write(outpath.toString());
						//br.newLine();
						//br.write(loadCmdStr.toString());
						//br.newLine();
						//br.write(filename);
						br.close();

						lChldProcOutStream.close();
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					unread = false;
					return true;
				}
				else
				{
					return false;
				}
			}
		};
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) 
	{
		return false;
	}

	}
