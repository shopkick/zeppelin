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

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.http.HttpTransportOptions;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * BigQuery interpreter for Zeppelin.
 * <p>
 * <ul>
 * <li>{@code zeppelin.bigquery.project_id} - Project ID in GCP</li>
 * <li>{@code zeppelin.bigquery.wait_time} - Query Timeout in ms</li>
 * <li>{@code zeppelin.bigquery.max_no_of_rows} - Max Result size</li>
 * </ul>
 * <p>
 * <p>
 * How to use: <br/>
 * {@code %bigquery.sql<br/>
 * {@code
 * SELECT departure_airport,count(case when departure_delay>0 then 1 else 0 end) as no_of_delays
 * FROM [bigquery-samples:airline_ontime_data.flights]
 * group by departure_airport
 * order by 2 desc
 * limit 10
 * }
 * </p>
 */

public class BigQueryRestrictedAccessInterpreter extends Interpreter {
  private static final String LEGACY_SQL = "#legacySQL";
  private static final int HTTP_TIMEOUT = 10000;
  private static Logger logger = LoggerFactory.getLogger(BigQueryInterpreter.class);
  private static BigQuery service = null;
  //Mutex created to create the singleton in thread-safe fashion.
  private static Object serviceLock = new Object();

  static final String PROJECT_ID = "zeppelin.bigquery.project_id";
  static final String WAIT_TIME = "zeppelin.bigquery.wait_time";
  static final String MAX_ROWS = "zeppelin.bigquery.max_no_of_rows";
  static final String TIME_ZONE = "zeppelin.bigquery.time_zone";
  static final String CREDENTIAL_FILE = "zeppelin.bigquery.restricted_interpreter_credentials";
  static final String ALLOWED_USERS = "zeppelin.bigquery.restricted_interpreter_users";

  private static final char NEWLINE = '\n';
  private static final char TAB = '\t';

  private static String jobId = null;

  private static final List<InterpreterCompletion> NO_COMPLETION = new ArrayList<>();

  public BigQueryRestrictedAccessInterpreter(Properties property) {
    super(property);
  }


  //Function to return valid BigQuery Service
  @Override
  public void open() {
    if (service == null) {
      synchronized (serviceLock) {
        if (service == null) {
          try {
            service = createAuthorizedClient();
            logger.info("Opened BigQuery SQL Connection");
          } catch (IOException e) {
            logger.error("Cannot open connection", e);
            close();
          }
        }
      }
    }
  }


  private Boolean isAllowed(String userId) {

    if (this.getProperty(ALLOWED_USERS) == null) {
      return Boolean.TRUE;
    }
    logger.info(" Allowed user ids are : " + getProperty(ALLOWED_USERS));
    if (Arrays.asList(getProperty(ALLOWED_USERS).split(",")).contains(userId)) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }


  //Function that Creates an authorized client to Google Bigquery.
  private BigQuery createAuthorizedClient() throws IOException {
    HttpTransportOptions httpOptions = HttpTransportOptions.newBuilder()
            .setConnectTimeout(HTTP_TIMEOUT)
            .setReadTimeout(HTTP_TIMEOUT).build();

    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
            .setTransportOptions(httpOptions)
            .setProjectId(getProperty(PROJECT_ID));

    if (getProperty(CREDENTIAL_FILE) != null) {
      GoogleCredentials credentials;
      try (FileInputStream serviceAccountStream = new FileInputStream(
              getProperty(CREDENTIAL_FILE))) {
        credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
      }
      builder.setCredentials(credentials);
    }
    if (isAllowed(this.getUserName()) == Boolean.TRUE) {
      return builder.build().getService();
    } else {
      throw new IOException("User " + getUserName() + " does not have access to this interpreter");
    }
  }

  public static String getFormattedString(String value, String type, String tZone) {
    switch (type) {
        case "TIMESTAMP":
          try {
            long tsTest = Double.valueOf(value).longValue() * 1000;
            Date date = new Date(tsTest);
            DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

            if (tZone == null || tZone == "") {
              tZone = "Pacific/Truk";
            }

            format.setTimeZone(TimeZone.getTimeZone(tZone));
            logger.debug("fix getFormattedString formatted field value  is {}", value);
            return format.format(date);
          } catch (Exception e) {
            logger.error("Exception occurred {}", e);
          }
          break;
        default:
          break;
    }
    return value.replace('\n', ' ').replace('\t', ' ');
  }



  private InterpreterResult executeSql(String sql) {
    try {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, runQuery(sql));
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  private String runQuery(String query) throws InterruptedException {
    boolean useLegacy = query.trim().startsWith(LEGACY_SQL);
    QueryJobConfiguration jobConfiguration = QueryJobConfiguration
            .newBuilder(query)
            .setUseLegacySql(useLegacy)
            .build();
    JobInfo jobInfo = JobInfo.newBuilder(jobConfiguration).build();
    try {
      Job job = service.create(jobInfo);
      jobId = job.getJobId().getJob();
      BigQuery.QueryResultsOption resultsOption = BigQuery.QueryResultsOption
              .maxWaitTime(Long.valueOf(getProperty(WAIT_TIME)));

      TableResult tableResult = job.getQueryResults(resultsOption);
      return formatResult(tableResult);
    } catch (NullPointerException n){
      throw new InterruptedException(
              "User " + getUserName() + " does not have access to this interpreter");
    }
  }

  private String formatResult(TableResult result) {

    StringBuilder strResponse = new StringBuilder("%table ");
    FieldList fields = result.getSchema().getFields();
    for (Field field : fields) {
      strResponse.append(field.getName()).append(TAB);
    }
    strResponse.setLength(strResponse.length() - 1);
    strResponse.append(NEWLINE);
    int maxRows = Integer.parseInt(getProperty(MAX_ROWS));
    int rowNum = 0, colNum;
    String tZone = getProperty(TIME_ZONE);
    while (true) {
      Iterable<FieldValueList> rows = result.getValues();
      for (FieldValueList row : rows) {
        if (rowNum++ >= maxRows) {
          break;
        }
        colNum = 0;
        for (FieldValue fieldValue : row) {
          String formattedField;
          if (fieldValue.isNull()) {
            formattedField = "<NULL>";
          } else {
            formattedField = getFormattedString(
                    fieldValue.getStringValue(),
                    fields.get(colNum).getType().name(),
                    tZone);
          }
          colNum++;
          strResponse.append(formattedField).append(TAB);
        }
        strResponse.setLength(strResponse.length() - 1);
        strResponse.append(NEWLINE);
      }
      if (!result.hasNextPage() || rowNum >= maxRows) {
        break;
      }
      result = result.getNextPage();
    }
    return strResponse.toString();
  }

  @Override
  public void close() {
    logger.info("Close bqsql connection!");
    service = null;
  }

  @Override
  public InterpreterResult interpret(String sql, InterpreterContext contextInterpreter) {
    logger.info("Run SQL command '{}'", sql);
    return executeSql(sql);
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
            BigQueryInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public void cancel(InterpreterContext context) {

    logger.info("Trying to Cancel current query statement.");

    if (service != null && jobId != null) {
      boolean cancel = service.cancel(jobId);
      if (cancel) {
        logger.info("Cancel job succeeded: ", jobId);
      } else {
        logger.warn("Cancel job failed: ", jobId);
      }
    } else {
      logger.info("Query Execution was already cancelled");
    }
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return NO_COMPLETION;
  }

}
