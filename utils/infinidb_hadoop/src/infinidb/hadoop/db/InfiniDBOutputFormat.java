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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.lib.db.*;


/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p> 
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where 
 * key has a type extending DBWritable. Returned {@link RecordWriter} 
 * writes <b>only the key</b> to the database with a batch SQL query.  
 * 
 */
public class InfiniDBOutputFormat<K  /*extends DBWritable*/, V> 
implements OutputFormat<K,V> {

  private static final Log LOG = LogFactory.getLog(InfiniDBOutputFormat.class);

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  protected class DBRecordWriter 
  implements RecordWriter<K, V> {

    private Connection connection;
    private PreparedStatement statement;

    protected DBRecordWriter() throws SQLException 
    {}

    /** {@inheritDoc} */
    public void close(Reporter reporter) throws IOException 
    {}

    /** {@inheritDoc} */
    public void write(K key, V value) throws IOException 
    {}
  }

  /** {@inheritDoc} */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
  throws IOException 
  {}


/** {@inheritDoc} */
public RecordWriter<K, V> getRecordWriter(FileSystem filesystem,
      JobConf job, String name, Progressable progress) throws IOException 
{
	try {
		return new DBRecordWriter();
	}
	catch (Exception ex) {
		throw new IOException(ex.getMessage());
	}
}

/**
  * Initializes the reduce-part of the job with the appropriate output settings
  * 
  * @param job
  *          The job
  * @param tableName
  *          The table to insert data into
  * @param fieldNames
  *          The field names in the table. If unknown, supply the appropriate
  *          number of nulls.
  */
public static void setOutput(JobConf job, String schemaName, String ... tableNames) 
{
	job.setOutputFormat(InfiniDBOutputFormat.class);
	job.setReduceSpeculativeExecution(false);

	InfiniDBConfiguration dbConf = new InfiniDBConfiguration(job);
	dbConf.setOutputSchemaName(schemaName);
	dbConf.setOutputTableNames(tableNames);
}

/**
 * Initializes the reduce-part of the job with the appropriate output settings
 * 
 * @param job
 *          The job
 * @param tableName
 *          The table to insert data into
 * @param fieldNames
 *          The field names in the table. If unknown, supply the appropriate
 *          number of nulls.
 */
public static void setOutput(JobConf job, String schemaName) 
{
	job.setOutputFormat(InfiniDBOutputFormat.class);
	job.setReduceSpeculativeExecution(false);

	InfiniDBConfiguration dbConf = new InfiniDBConfiguration(job);

	dbConf.setOutputSchemaName(schemaName);
}
}

