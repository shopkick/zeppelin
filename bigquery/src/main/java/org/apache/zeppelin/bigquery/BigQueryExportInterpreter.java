/*
* Copyright 2016 Google Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0

* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.bigquery;


import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.UUID;


import com.google.cloud.bigquery.*;
import com.google.cloud.http.HttpTransportOptions;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interpreter to export BigQuery output to GCS bucket for Zeppelin.
 *
 * <ul>
 * <li>{@code zeppelin.bigquery.project_id} - Project ID in GCP</li>
 * <li>{@code zeppelin.bigquery.wait_time} - Query Timeout in ms</li>
 * <li>{@code zeppelin.bigquery.max_no_of_rows} - Max Result size</li>
 * </ul>
 *
 * <p>
 * How to use: <br/>
 * {@code %bigquery.sql<br/>
 * {@code
 *  SELECT departure_airport,count(case when departure_delay>0 then 1 else 0 end) as no_of_delays
 *  FROM [bigquery-samples:airline_ontime_data.flights]
 *  group by departure_airport
 *  order by 2 desc
 *  limit 10
 * }
 * </p>
 *
 */

public class BigQueryExportInterpreter extends BigQueryInterpreter {


  private static final String DOWNLOAD_LINK_PREFIX = "https://storage.cloud.google.com";
  private static final String COMPRESSION_SUFFIX = "gzip";
  static final String EXPORT_BUCKET = "zeppelin.bigquery.export.bucket";
  static final String EXPORT_DATASET = "zeppelin.bigquery.export.dataset";

  public BigQueryExportInterpreter(Properties property) {
    super(property);
  }


  //Function to call bigQuery to run SQL and return results to the Interpreter for output
  private InterpreterResult execute(String sql, String noteId, String paragraphId) {
    DateFormat formatter = new SimpleDateFormat("yyyyMMdd_hhmmss_SSS");
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(System.currentTimeMillis());
    String destTable = String.format("%s_%s_%s",
        noteId,
        paragraphId.replace("-", "_"),
        formatter.format(calendar.getTime()));
    TableId destTableId = TableId.of(getProperty(PROJECT_ID),
        getProperty(EXPORT_DATASET),
        destTable);
    String destUrl = String.format("gs://%s/%s.%s",
        getProperty(EXPORT_BUCKET),
        destTable,
        COMPRESSION_SUFFIX);
    try {
      runQuery(sql, destTableId);
      exportTable(destTableId, destUrl);
    } catch (InterruptedException | ZeppelinBigQueryException e) {
      logger.error(e.getMessage());
      return new InterpreterResult(Code.ERROR, e.getMessage());
    } finally {
      service.delete(destTableId);
    }
    return new InterpreterResult(Code.SUCCESS,
        formatResult(getProperty(EXPORT_BUCKET), destTable));
  }

  private void runQuery(String query, TableId destTableId)
      throws InterruptedException, ZeppelinBigQueryException {
    boolean useLegacy = query.trim().startsWith(LEGACY_SQL);
    QueryJobConfiguration jobConfiguration = QueryJobConfiguration
        .newBuilder(query)
        .setUseLegacySql(useLegacy)
        .setAllowLargeResults(true)
        .setDestinationTable(destTableId)
        .build();
    JobInfo jobInfo = JobInfo.newBuilder(jobConfiguration)
        .setJobId(JobId.of(UUID.randomUUID().toString()))
        .build();
    Job job = service.create(jobInfo);
    jobId = job.getJobId().getJob();
    job = job.waitFor();
    if (job == null) {
      throw new ZeppelinBigQueryException("Query job doesn't exist anymore");
    } else if (job.getStatus().getError() != null) {
      throw new ZeppelinBigQueryException(job.getStatus().getError().getMessage());
    }
  }

  private void exportTable(TableId tableId, String destUrl)
      throws InterruptedException, ZeppelinBigQueryException {
    ExtractJobConfiguration jobConfiguration = ExtractJobConfiguration
        .newBuilder(tableId, destUrl)
        .setFormat("CSV")
        .setCompression("GZIP")
        .build();
    JobInfo jobInfo = JobInfo.newBuilder(jobConfiguration)
        .setJobId(JobId.of(UUID.randomUUID().toString()))
        .build();
    Job job = service.create(jobInfo);
    jobId = job.getJobId().getJob();
    job = job.waitFor();
    if (job == null) {
      throw new ZeppelinBigQueryException("Extract job doesn't exist anymore");
    } else if (job.getStatus().getError() != null) {
      throw new ZeppelinBigQueryException(job.getStatus().getError().getMessage());
    }
  }

  private String formatResult(String bucket, String table) {
    return String.format("%%html <a href=\"%s/%s/%s.%s\" target=\"_blank\">Click to Download</a>",
        DOWNLOAD_LINK_PREFIX,
        bucket,
        table,
        COMPRESSION_SUFFIX);
  }

  @Override
  public InterpreterResult interpret(String sql, InterpreterContext contextInterpreter) {
    logger.info("Run SQL command '{}'", sql);
    return execute(sql, contextInterpreter.getNoteId(), contextInterpreter.getParagraphId());
  }


}
