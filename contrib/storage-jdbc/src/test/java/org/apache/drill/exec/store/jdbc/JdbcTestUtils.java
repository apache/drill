package org.apache.drill.exec.store.jdbc;

import org.apache.commons.lang3.RandomStringUtils;

public class JdbcTestUtils {


  /**
   * Generates a random string of length n
   * @param length Length of desired string
   * @return Random string
   */
  public static String generateRandomString(int length) {
    return RandomStringUtils.random(length, true, false);
  }

  /**
   * Generates a CSV file of random data with the desired number of columns and rows. The
   * data will be a mix of numbers and text
   * @param columnCount Number of columns
   * @param rowCount Number of rows
   * @return The path of the generated file
   */
  public static String generateCsvFile(int columnCount, int rowCount) {

  }

  /**
   * Deletes the file
   * @param filepath The file to be deleted
   */
  public static void deleteCsvFile(String filepath) {

  }
}
