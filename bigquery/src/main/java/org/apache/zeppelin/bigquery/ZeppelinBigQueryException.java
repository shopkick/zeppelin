package org.apache.zeppelin.bigquery;

/**
 * Exception to be used with Interpreter for exporting BigQuery output to GCS bucket.
 */

public class ZeppelinBigQueryException extends Exception {
  public ZeppelinBigQueryException(String message) {
    super(message);
  }
}
