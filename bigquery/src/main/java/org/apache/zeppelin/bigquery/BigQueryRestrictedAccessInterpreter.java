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
import java.util.Arrays;
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

public class BigQueryRestrictedAccessInterpreter extends BigQueryInterpreter {

  // This property gives path to the restricted interpreter service account json
  private final String CREDENTIAL_FILE = "zeppelin.bigquery.restricted_interpreter_credentials";

  // This property gives comma separated values of authorized users
  static final String ALLOWED_USERS = "zeppelin.bigquery.restricted_interpreter_users";

  public BigQueryRestrictedAccessInterpreter(Properties property) {
    super(property);
  }

  private boolean isAllowed(String userId) {
    if (this.getProperty(ALLOWED_USERS) == null) {
      return true;
    }
    logger.info(" Allowed user ids are : " + getProperty(ALLOWED_USERS));
    return (Arrays.asList(getProperty(ALLOWED_USERS).split(",")).contains(userId));
  }


  @Override
  //Function that Creates an authorized client to Google Bigquery.
  protected BigQuery createAuthorizedClient() throws IOException {
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

}
