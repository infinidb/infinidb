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

*/

//  $Id$

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

