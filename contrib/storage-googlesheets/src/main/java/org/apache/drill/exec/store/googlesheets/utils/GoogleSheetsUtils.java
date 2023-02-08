/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.googlesheets.utils;

import com.google.api.client.auth.oauth2.AuthorizationCodeFlow;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.DataStoreCredentialRefreshListener;
import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.DataStore;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.Sheets.Spreadsheets.Values.BatchGet;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.DeleteSheetRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.Typifier;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.googlesheets.DrillDataStore;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsColumn;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePluginConfig;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnRange;
import org.apache.drill.exec.util.Utilities;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.google.api.client.util.Strings.isNullOrEmpty;


public class GoogleSheetsUtils {

  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsUtils.class);
  private static final int SAMPLE_SIZE = 5;
  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
  private static final String UNKNOWN_HEADER = "field_";
  private static final String APPLICATION_NAME = "Drill";

  /**
   * Represents the possible data types found in a GoogleSheets document
   */
  public enum DATA_TYPES {
    /**
     * Represents a field before the datatype is known
     */
    UNKNOWN,
    /**
     * A numeric data type, either a float or an int.  These are all
     * converted to Doubles when projected.
     */
    NUMERIC,
    /**
     * A string data type
     */
    VARCHAR,
    /**
     * A field containing a date
     */
    DATE,
    /**
     * A field containing a time
     */
    TIME,
    /**
     * A field containing timestamps.
     */
    TIMESTAMP,
    /**
     * A field containing a list of strings.  Only used for implicit columns.
     */
    VARCHAR_REPEATED
  }

  /**
   * Creates an authorized {@link Credential} for use in GoogleSheets queries.
   * @param config The {@link GoogleSheetsStoragePluginConfig} to be authorized
   * @param dataStore A {@link DrillDataStore} containing the user's tokens
   * @param queryUser The current query user's ID.  This should be set to anonymous if user translation is disabled.
   * @return A validated {@link Credential} object.
   * @throws IOException If anything goes wrong
   * @throws GeneralSecurityException If the credentials are invalid
   */
  public static Credential authorize(GoogleSheetsStoragePluginConfig config,
                                     DataStore<StoredCredential> dataStore,
                                     String queryUser) throws IOException, GeneralSecurityException {
    GoogleClientSecrets clientSecrets = config.getSecrets();
    GoogleAuthorizationCodeFlow flow;
    List<String> scopes = Collections.singletonList(SheetsScopes.SPREADSHEETS);

    if (dataStore == null) {
      logger.debug("Datastore is null");
      throw UserException.connectionError()
        .message("The DrillDataStore is null.  This should not happen.")
        .build(logger);
    } else if (dataStore.getDataStoreFactory() == null) {
      logger.debug("Datastore factory is null");
      throw UserException.connectionError()
        .message("The DrillDataStoreFactory is null.  This should not happen.")
        .build(logger);
    }

    flow = new GoogleAuthorizationCodeFlow.Builder
      (GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, clientSecrets, scopes)
        .setDataStoreFactory(dataStore.getDataStoreFactory())
        .setAccessType("offline")
        .build();

    return loadCredential(queryUser, flow, dataStore);
  }

  public static Credential loadCredential(String userId, GoogleAuthorizationCodeFlow flow, DataStore<StoredCredential> credentialDataStore) {
    // No requests need to be performed when userId is not specified.
    if (isNullOrEmpty(userId)) {
      return null;
    }

    if (credentialDataStore == null) {
      return null;
    }
    Credential credential = newCredential(userId, flow, credentialDataStore);
    StoredCredential stored = ((DrillDataStore<StoredCredential>)credentialDataStore).getStoredCredential();
    if (stored == null) {
      return null;
    }
    credential.setAccessToken(stored.getAccessToken());
    credential.setRefreshToken(stored.getRefreshToken());
    credential.setExpirationTimeMilliseconds(stored.getExpirationTimeMilliseconds());

    return credential;
  }

  /**
   * Returns a new credential instance based on the given user ID.
   *
   * @param userId user ID or {@code null} if not using a persisted credential store
   */
  private static Credential newCredential(String userId, AuthorizationCodeFlow flow, DataStore<StoredCredential> credentialDataStore) {
    Credential.Builder builder =
      new Credential.Builder(flow.getMethod())
        .setTransport(flow.getTransport())
        .setJsonFactory(flow.getJsonFactory())
        .setTokenServerEncodedUrl(flow.getTokenServerEncodedUrl())
        .setClientAuthentication(flow.getClientAuthentication())
        .setRequestInitializer(flow.getRequestInitializer())
        .setClock(flow.getClock());

    if (credentialDataStore != null) {
      builder.addRefreshListener(
        new DataStoreCredentialRefreshListener(userId, credentialDataStore));
    }
    builder.getRefreshListeners().addAll(flow.getRefreshListeners());
    return builder.build();
  }



  public static Sheets getSheetsService(GoogleSheetsStoragePluginConfig config,
                                        DataStore<StoredCredential> dataStore,
                                        String queryUser)
    throws IOException, GeneralSecurityException {
    Credential credential = GoogleSheetsUtils.authorize(config, dataStore, queryUser);
    return new Sheets.Builder(
      GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance(), credential)
      .setApplicationName(APPLICATION_NAME)
      .build();
  }

   /** Returns an authenticated {@link Drive} service.
   * @param config The {@link GoogleSheetsStoragePluginConfig} for the plugin
   * @param dataStore A {@link DrillDataStore} for the stored credentials
   * @param queryUser A {@link String} containing the current query user.
   * @return An authenticated {@link Drive} service.
   * @throws IOException If anything goes wrong throw an IOException
   * @throws GeneralSecurityException If the creds are invalid or
   */
  public static Drive getDriveService(GoogleSheetsStoragePluginConfig config,
                                      DataStore<StoredCredential> dataStore,
                                      String queryUser) throws IOException, GeneralSecurityException {
    Credential credential = GoogleSheetsUtils.authorize(config, dataStore, queryUser);
    return new Drive.Builder(
      GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance(), credential)
      .setApplicationName(APPLICATION_NAME)
      .build();
  }

  /**
   * In GoogleSheets, the file is uniquely identified by a non-human readable token. The Sheets SDK does
   * not provide a means to map tokens to file names.  To do this, we need to use the Google Drive SDK.
   *
   * Google Drive's concept of folders is more similar to that of S3 which doesn't really have folders or
   * directories. So the user is not able to include any file paths in the query string.
   *
   * More importantly however, is that Google Drive allows duplicate file names, even within the same directory.
   * Thus, it is entirely possible to have an entire directory of files with the same name. The tokens for these files
   * will be different, but the human-readable names can be the same.  This creates an obvious problem for Drill as we
   * need unique file names to include in a query.
   *
   * @param driveService An authenticated {@link Drive} service.
   * @return A {@link HashMap} containing the tokens as keys and the file names as values.
   * @throws IOException If anything goes wrong, throw an IOException.
   */
  public static Map<String,String> getTokenToNameMap(Drive driveService) throws IOException {
    Map<String, String> sheetMapping = new HashMap<>();
    String pageToken = null;

    do {
      FileList result = driveService.files().list()
        .setQ("mimeType='application/vnd.google-apps.spreadsheet'")
        .setSpaces("drive")
        .setPageToken(pageToken)
        .execute();

      for (File file : result.getFiles()) {
        sheetMapping.put(file.getId(), file.getName());
      }

      pageToken = result.getNextPageToken();
    } while (pageToken != null);

    return sheetMapping;
  }

  /**
   * Google Sheets tokens are strings of length 44 that contain upper and lower case letters, numbers and underscores.
   * This function will attempt to identify file tokens.
   * <p>
   * Given that Google's spec for file IDs is not officially published, and can change at any time, we will keep the
   * validation as light as possible to prevent future issues, in the event Google changes their file Id structure.
   * @param id A {@link String} containing an unknown identifier
   * @return True if the string is a file probable file token, false if not.
   */
  public static boolean isProbableFileToken(String id) {
    logger.debug("Checking token {}", id);
    if (StringUtils.isEmpty(id)) {
      return false;
    } else if (id.length() != 44) {
      return false;
    } else {
      Pattern pattern = Pattern.compile("[0-9][a-zA-Z0-9_-]{43}");
      return pattern.matcher(id).find();
    }
  }

  /**
   * Returns a list of the titles of the available spreadsheets within a given Google sheet.
   * @param service The Google Sheets service
   * @param sheetID The sheetID for the Google sheet.  This can be obtained from the URL of your Google sheet
   * @return A list of spreadsheet names within a given Google Sheet
   * @throws IOException If the Google sheet is unreachable or invalid.
   */
  public static List<Sheet> getTabList(Sheets service, String sheetID) throws IOException {
    logger.debug("Getting tabs for: {}", sheetID);
    Spreadsheet spreadsheet = service.spreadsheets().get(sheetID).execute();
    return spreadsheet.getSheets();
  }

  /**
   * Converts a column index to A1 notation. Google sheets has a limitation of approx 18k
   * columns, but that is not enforced here. The column index must be greater than zero or
   * the function will return null.
   *
   * References code found here:
   * <a href="https://stackoverflow.com/questions/21229180/convert-column-index-into-corresponding-column-letter">Stack Overflow Article</a>
   * @param column The column index for the desired column. Must be greater than zero
   * @return The A1 representation of the column index.
   */
  public static String columnToLetter(int column) {
    if (column <= 0) {
      return null;
    }

    int temp;
    StringBuilder letter = new StringBuilder();
    while (column > 0) {
      temp = (column - 1) % 26;
      letter.insert(0, (char) (temp + 65));
      column = (column - temp - 1) / 26;
    }
    return letter.toString();
  }

  /**
   * Given a column reference in A1 notation, this function will
   * return the column numeric index. GoogleSheets has a limit of approx
   * 18k columns, but that is not enforced here.
   *
   * References code found here:
   * <a href="https://stackoverflow.com/questions/21229180/convert-column-index-into-corresponding-column-letter">Stack Overflow Article</a>
   * @param letter The desired column in A1 notation
   * @return The index of the supplied column
   */
  public static int letterToColumnIndex(String letter) {
    // Make sure the letters are all upper case.
    letter = letter.toUpperCase();
    int column = 0;
    int length = letter.length();
    for (int i = 0; i < length; i++) {
      column += (Character.codePointAt(letter, i) - 64) * (int)Math.pow(26, length - i - 1);
    }
    return column;
  }

  /**
   * This function will be used to build the schema for Drill.  As Google sheets does
   * @param service An authenticated Google Sheets Service
   * @param sheetID The Sheet ID for the Google Sheet (Can be found in the Sheet URL)
   * @param tabName The tab name of the actual spreadsheet you want to query
   * @return A nested list of the first five rows of the dataset.
   * @throws IOException If the request fails, throw an IOException
   */
  public static List<List<Object>> getFirstRows (Sheets service, String sheetID, String tabName) throws IOException {
    String range = tabName + "!1:" + SAMPLE_SIZE;
    return service.spreadsheets().values().get(sheetID, range).execute().getValues();
  }

  /**
   * Returns a 2D table of Objects representing the given range in A1 notation. Note that this
   * function cannot be used with multiple ranges.  If you are trying to retrieve multiple groups
   * of columns, you must use the getBatchData function.
   * @param service The Authenticated GoogleSheets service
   * @param sheetID The GoogleSheet ID.  This can be found in the Sheet URL
   * @param range The range in A1 notation.
   * @return  A 2D table of Objects representing the given range.
   * @throws IOException If the request fails, throw an IOException.
   */
  public static List<List<Object>> getDataFromRange(Sheets service, String sheetID, String range) throws IOException {
    return service.spreadsheets().values().get(sheetID, range).execute().getValues();
  }

  /**
   * Finds a {@link Sheet} from a list of tabs with a given title.  If the sheet is not present,
   * the function will throw a User Exception.
   * @param tabName The name of the desired sheet.
   * @param tabList A {@link List} of {@link Sheet} objects
   * @return The desired Sheet.
   */
  public static Sheet getSheetFromTabList(String tabName, List<Sheet> tabList) {
    for (Sheet sheet : tabList) {
      if (sheet.getProperties().getTitle().contentEquals(tabName)) {
        return sheet;
      }
    }

    throw UserException.dataReadError()
        .message("Could not find sheet " + tabName)
        .build(logger);
  }

  /**
   * This function is used to get data when projection is pushed down to Google Sheets.
   * @param service The Authenticated GoogleSheets service
   * @param sheetID The GoogleSheet ID.  This can be found in the Sheet URL
   * @param ranges The list of ranges
   * @throws IOException If anything goes wrong, IOException will be thrown
   */
  public static List<List<Object>> getBatchData(Sheets service, String sheetID, List<String> ranges) throws IOException {
    logger.debug("Getting ranges: {}", ranges);
    BatchGet request = service.spreadsheets().values().batchGet(sheetID).setRanges(ranges);
    List<ValueRange> response = request.execute().getValueRanges();

    List<List<Object>> results  = new ArrayList<>();
    // In Google's infinite wisdom when designing this API, the results
    // are returned in a completely different fashion than when projection is not
    // pushed down to Google Sheets. Specifically, if you use the regular values() to retrieve
    // values from a GoogleSheet, you get a List of rows.  Whereas if you use the BatchGet,
    // you get a list of columns, sort of.  Except these columns are embedded in a bunch of
    // other debris from which you must extract the actual data.
    //
    // It should be noted that the GoogleSheets API does not accept multiple ranges in the
    // request, so it is necessary to use the batch request.
    for (int rowIndex = 0; rowIndex < ((ArrayList<?>) response.get(0).get("values")).size(); rowIndex++) {
      List<Object> row = new ArrayList<>();
      for (int colIndex = 0; colIndex < response.size(); colIndex++) {
        try {
          Object value = ((ArrayList<?>) ((ArrayList<?>) response.get(colIndex).get("values")).get(rowIndex)).get(0);
          row.add(value);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
          row.add(null);
        }
      }
      results.add(row);
    }
    return results;
  }

  /**
   *
   * @param sampleData This represents a sample of the first few rows of data which will be used to build the schema.
   * @param projectedColumns A list of projected columns
   * @param allTextMode If true, the columns will all be of the VARCHAR type
   * @return A map of the column name and {@link GoogleSheetsColumn} column for every projected column.
   */
  public static Map<String, GoogleSheetsColumn> getColumnMap(List<List<Object>> sampleData, List<SchemaPath> projectedColumns, boolean allTextMode) {
    // For now, we assume that the column headers are in the first row
    int emptyColumnCount = 0;
    List<String> headers = new ArrayList<>();
    Map<String, DATA_TYPES> dataTypes = new HashMap<>();

    for (Object rawHeader : sampleData.get(0)) {
      String header = (String) rawHeader;

      // If the header row is empty, assign a value of `field_n` where n is the unknown header count.
      if (Strings.isNullOrEmpty(header)) {
        header = UNKNOWN_HEADER + emptyColumnCount;
        emptyColumnCount++;
      }
      headers.add(header);
      if (allTextMode) {
        dataTypes.put(header, DATA_TYPES.VARCHAR);
      } else {
        dataTypes.put(header, DATA_TYPES.UNKNOWN);
      }
    }

    if (!allTextMode) {
      for (int rowIndex = 1; rowIndex < sampleData.size(); rowIndex++) {
        for (int colIndex = 0; colIndex < sampleData.get(rowIndex).size(); colIndex++) {
          updateDataType(headers.get(colIndex), dataTypes, sampleData.get(rowIndex).get(colIndex).toString());
        }
      }
    }
    // At this point, we have inferred the columns.  We will return a list of {@link GoogleSheetsColumn} which
    // we will need later for projection pushdown and other schema creation activities.
    Map<String, GoogleSheetsColumn> columnMap = new LinkedHashMap<>();
    int colCount = 0;
    for (String header: headers) {
      // When building the schema map, we only want to include projected columns.  This will be important later
      // when we build the range request.
      if (Utilities.isStarQuery(projectedColumns) || isProjected(projectedColumns, header)) {
        GoogleSheetsColumn column = new GoogleSheetsColumn(header, dataTypes.get(header), headers.indexOf(header), colCount);
        columnMap.put(header, column);
        colCount++;
      }
    }
    return columnMap;
  }

  public static List<GoogleSheetsColumnRange> getProjectedRanges(String sheetName, Map<String, GoogleSheetsColumn> columnMap) {
    List<GoogleSheetsColumnRange> projectedRanges = new ArrayList<>();
    int lastIndex = -1;
    int currentIndex;
    GoogleSheetsColumnRange currentRange = new GoogleSheetsColumnRange(sheetName);
    for (GoogleSheetsColumn column : columnMap.values()) {
      // Exclude metadata columns
      if (column.isMetadata()) {
        continue;
      }
      currentIndex = column.getColumnIndex();

      // Edge case for first range
      if (currentRange.getStartColIndex() == null) {
        currentRange = currentRange.setStartIndex(currentIndex);
      }

      // End the range and create a new one.
      if (currentIndex != (lastIndex + 1) && lastIndex != -1) {
        currentRange.setEndIndex(lastIndex);
        projectedRanges.add(currentRange);
        currentRange = new GoogleSheetsColumnRange(sheetName)
          .setStartIndex(currentIndex);
      }
      lastIndex = currentIndex;
    }
    currentRange = currentRange.setEndIndex(lastIndex);
    projectedRanges.add(currentRange);
    return projectedRanges;
  }

  /**
   * Returns true if the column is projected, false if not.
   * @param projectedColumns A list of projected columns AKA the haystack.
   * @param columnName The column name AKA the needle
   * @return True if the needle is in the haystack, false if not.
   */
  public static boolean isProjected(List<SchemaPath> projectedColumns, String columnName) {
    // Star queries project everything, so return true.  Technically this
    // might not always be correct, in the case that a query projects a non-existent column.
    if (Utilities.isStarQuery(projectedColumns)) {
      return true;
    }
    for (SchemaPath path : projectedColumns) {
      if (path.getAsNamePart().getName().contains(columnName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a Drill Schema from a Map of GoogleSheetsColumns.
   * @param columnMap A map of {@link GoogleSheetsColumn} containing the schema info.
   * @return A populated {@link TupleMetadata} schema
   */
  public static TupleMetadata buildSchema(Map<String, GoogleSheetsColumn> columnMap) {
    SchemaBuilder builder = new SchemaBuilder();
    for (GoogleSheetsColumn column : columnMap.values()) {
      builder.addNullable(column.getColumnName(), column.getDrillDataType());
    }
    return builder.build();
  }

  /**
   * Infers the datatype of an unknown string.
   * @param data An input String of unknown type.
   * @return The {@link DATA_TYPES} of the unknown string.
   */
  public static DATA_TYPES inferDataType (String data) {
    Entry<Class, String> result = Typifier.typify(data);
    String dataType = result.getKey().getSimpleName();

    // If the string is empty, return UNKNOWN
    if (StringUtils.isEmpty(data)) {
      return DATA_TYPES.UNKNOWN;
    } else if (dataType.equalsIgnoreCase("Double")) {
      return DATA_TYPES.NUMERIC;
    } else if(dataType.equalsIgnoreCase("LocalDateTime")) {
      return DATA_TYPES.TIMESTAMP;
    } else if (dataType.equalsIgnoreCase("LocalDate")) {
      return DATA_TYPES.DATE;
    } else if (dataType.equalsIgnoreCase("LocalTime")) {
      return DATA_TYPES.TIME;
    } else {
      return DATA_TYPES.VARCHAR;
    }
  }

  public static void updateDataType(String columnName, Map<String, DATA_TYPES> dataTypesMap, String value) {
    if (StringUtils.isEmpty(value)) {
      return;
    }

    // Get the data type of the unknown value
    DATA_TYPES probableDataType = inferDataType(value);
    DATA_TYPES columnDataType = dataTypesMap.get(columnName);

    // If the column data type matches the new value's data type, make no changes
    if (probableDataType == columnDataType) {
      return;
    }

    // If the column was unknown assign it the data type that we found
    if (columnDataType == DATA_TYPES.UNKNOWN) {
      dataTypesMap.put(columnName, probableDataType);
    } else if (columnDataType == DATA_TYPES.NUMERIC && probableDataType == DATA_TYPES.VARCHAR) {
      // If we have a column that is thought to be numeric, we will continue to consider it numeric unless
      // we encounter a string, at which point we will convert it to a String.
      dataTypesMap.put(columnName, DATA_TYPES.VARCHAR);
    }
  }

  /**
   * Adds a new tab to an existing GoogleSheet document.
   *
   * @param service   An authenticated GoogleSheet service
   * @param sheetName The GoogleSheet name of the document
   * @param tabName   The name of the tab you wish to add to the GoogleSheet document
   * @throws IOException Throws an IOException if anything goes wrong.
   */
  public static void addTabToGoogleSheet(Sheets service, String sheetName, String tabName)
    throws IOException {
    List<Request> requests = new ArrayList<>();
    requests.add(new Request()
      .setAddSheet(new AddSheetRequest().setProperties(new SheetProperties()
        .setTitle(tabName)
        .setIndex(0))));
    BatchUpdateSpreadsheetRequest body = new BatchUpdateSpreadsheetRequest().setRequests(requests);
    service.spreadsheets().batchUpdate(sheetName, body).execute();
  }

  /**
   * Removes a sheet from an existing GoogleSheets document.  This method should only be used if the GoogleSheets
   * document has more than one tab.
   * @param service An authenticated GoogleSheet {@link Sheets}
   * @param fileToken The File token of the GoogleSheet containing the sheet to be deleted
   * @param deletedTab  A {@link Sheet} which will be removed
   * @throws IOException If anything goes wrong.
   */
  public static void removeTabFromGoogleSheet(Sheets service, String fileToken, Sheet deletedTab) throws IOException {
    List<Request> requests = new ArrayList<>();
    requests.add(new Request()
        .setDeleteSheet(new DeleteSheetRequest().setSheetId(deletedTab.getProperties().getSheetId()))
    );

    BatchUpdateSpreadsheetRequest body = new BatchUpdateSpreadsheetRequest().setRequests(requests);
    service.spreadsheets().batchUpdate(fileToken, body).execute();
  }

  /**
   * Accepts a list of data and writes this data to a GoogleSheet document.
   * @param service An authenticated GoogleSheet service
   * @param sheetID The SheetID.  This can be obtained from the URL of the GoogleSheet Document
   * @param tabName The tab name within the aforementioned GoogleSheet
   * @param data A list of rows of the data to be inserted.
   * @throws IOException If anything goes wrong, throw an IO exception
   */
  public static void writeDataToGoogleSheet(Sheets service, String sheetID, String tabName, List<List<Object>> data)
    throws IOException {
    String range = tabName + "!A1";
    ValueRange body = new ValueRange()
      .setValues(data)
      .setMajorDimension("ROWS");

    UpdateValuesResponse result = service.spreadsheets().values().update(sheetID, range, body)
        .setValueInputOption("RAW")
        .execute();
  }

  /**
   * Accepts a list of data and writes this data to a GoogleSheet document.
   * @param service An authenticated GoogleSheet service
   * @param sheetID The SheetID.  This can be obtained from the URL of the GoogleSheet Document
   * @param tabName The tab name within the aforementioned GoogleSheet
   * @param data A list of rows of the data to be inserted.
   * @throws IOException If anything goes wrong, throw an IO exception
   */
  public static void appendDataToGoogleSheet(Sheets service, String sheetID, String tabName, List<List<Object>> data)
      throws IOException {
    String range = tabName + "!A1";
    ValueRange body = new ValueRange()
        .setValues(data)
        .setMajorDimension("ROWS");

    AppendValuesResponse result = service.spreadsheets().values().append(sheetID, range, body)
        .setValueInputOption("RAW")
        .execute();
  }
}
