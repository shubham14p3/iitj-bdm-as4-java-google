import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.v2.RowRange;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/*
 * Use Google Bigtable to store and analyze sensor data.
 */
public class Bigtable {
    // TODO: Fill in information for your database
    public final String projectId = "iitjdb";
    public final String instanceId = "ail7560";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "bdm_as4";
    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        Bigtable testbt = new Bigtable();
        testbt.run();
    }

    public void connect() {
        try {
            // Settinging up data client to interact with Bigtable
            BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                    .setProjectId("g23ai2028") // Project ID
                    .setInstanceId("g23ai2028") // Instance ID
                    .build();
            dataClient = BigtableDataClient.create(dataSettings);

            // Settinging up admin client to manage Bigtable resources
            BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                    .setProjectId("g23ai2028") // Project ID
                    .setInstanceId("g23ai2028") // Instance ID
                    .build();
            adminClient = BigtableTableAdminClient.create(adminSettings);

            System.out.println("Successfully connected to Bigtable instance: g23ai2028");
        } catch (Exception e) {
            System.out.println("Error: Unable to connect to Bigtable instance.");
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        System.out.println("Connecting .. ");
        connect();

        // TODO: Comment or uncomment these as you proceed. Once load data, comment them
        // out.
        System.out.println("deleting table .. ");
        deleteTable();
        System.out.println("Creating table .. ");
        createTable();
        System.out.println("Loading Data .. ");
        loadData();

        int temp = query1();
        System.out.println("Temperature: " + temp);

        int windspeed = query2();
        System.out.println("Windspeed: " + windspeed);

        ArrayList<Object[]> data = query3();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < data.size(); i++) {
            Object[] vals = data.get(i);
            for (int j = 0; j < vals.length; j++) {
                buf.append(vals[j].toString() + " ");
            }
            buf.append("\n");
        }
        System.out.println(buf.toString());

        temp = query4();
        System.out.println("Temperature: " + temp);

        query5();
        close();
    }

    /**
     * Close data and admin clients
     */
    public void close() {
        dataClient.close();
        adminClient.close();
    }

    public void createTable() {
        try {
            if (!adminClient.exists(tableId)) {
                CreateTableRequest request = CreateTableRequest.of(tableId)
                        .addFamily(COLUMN_FAMILY); // Adding single column family: "sensor"
                adminClient.createTable(request);
                System.out.println("Table created successfully: " + tableId);
            } else {
                System.out.println("Table already exists: " + tableId);
            }
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Loads data into database.
     * Data is in CSV files. Note that it must be converted to hourly data.
     * Takes first reading in an hour and ignores any others.
     */
    public void loadData() throws Exception {
        // Hardcoding  files paths for specific data files so no mismathc happns
        String[] files = {
                "data/portland.csv",
                "data/seatac.csv",
                "data/vancouver.csv"
        };

        System.out.println("Loading Data...");

        for (String filePath : files) {
            // Extracting station ID from file name
            String stationId = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
            System.out.println("Processing file: " + filePath);

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line = reader.readLine(); // Skiping header row so we can save data
                String lastHour = ""; // To track hourly data
                BulkMutation bulkMutation = BulkMutation.create(tableId); // Use bulk mutation for better detail view

                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");

                    if (fields.length < 9)
                        continue; // Skiping invalid rows

                    String date = fields[1]; // Date
                    String time = fields[2]; // Time
                    String hour = time.split(":")[0]; // Extracting hour

                    // Skiping non-hourly data
                    if (hour.equals(lastHour))
                        continue;
                    lastHour = hour;

                    // Constructing row key
                    String rowKey = stationId + "#" + date + "#" + hour;

                    // Adding mutation entry for current row
                    bulkMutation.add(RowMutationEntry.create(rowKey)
                            .setCell(COLUMN_FAMILY, "temperature", fields[3]) // Temperature
                            .setCell(COLUMN_FAMILY, "dewpoint", fields[4]) // Dewpoint
                            .setCell(COLUMN_FAMILY, "relhum", fields[5]) // Relative Humidity
                            .setCell(COLUMN_FAMILY, "speed", fields[6]) // Wind Speed
                            .setCell(COLUMN_FAMILY, "gust", fields[7]) // Wind Gust
                            .setCell(COLUMN_FAMILY, "pressure", fields[8])); // Atmospheric Pressure
                }

                // Executing all mutations for current file
                dataClient.bulkMutateRows(bulkMutation);
                System.out.println("Data loaded successfully for station: " + stationId);
            } catch (IOException e) {
                System.err.println("Error reading file: " + filePath + " - " + e.getMessage());
            }
        }

        System.out.println("Data loading completed.");
    }

    /**
     * Query returns temperature at Vancouver on 2022-10-01 at 10 a.m.
     *
     * @return Temperature as an integer
     * @throws Exception if an error occurs
     */
    public int query1() throws Exception {
        System.out.println("Executing query #1.");

        // Constructing row key for Vancouver at specific date and time
        String stationId = "vancouver";
        String date = "2022-10-01";
        String hour = "10";
        String rowKey = stationId + "#" + date + "#" + hour;

        // Read row from Bigtable
        Row row = dataClient.readRow(tableId, rowKey);

        // If row is null, it means no data is available for given key
        if (row == null) {
            System.out.println("No data found for specified query.");
            return -1; // Return -1 or any other value to indicate no data
        }

        // Extracting temperature from "temperature" column in row
        String temperatureValue = "";
        for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
            temperatureValue = cell.getValue().toStringUtf8();
        }

        // Convert temperature value to integer
        int temperature = Integer.parseInt(temperatureValue);

        System.out.println("Temperature at Vancouver on 2022-10-01 at 10 a.m.: " + temperature);
        return temperature;
    }

    /**
     * Query returns highest wind speed in month of September 2022 in
     * Portland.
     * 
     * @return highest wind speed as an integer
     * @throws Exception if an error occurs
     */
    public int query2() throws Exception {
        System.out.println("Executing query #2.");

        // Station ID and date range for query
        String stationId = "portland";
        String startDate = "2022-09-01";
        String endDate = "2022-09-30";

        // Prefix for row keys to filter relevant data
        String prefix = stationId + "#2022-09";

        // Query rows with prefix for station in September 2022
        Query query = Query.create(tableId).prefix(prefix);

        // Variable to store highest wind speed
        int maxWindSpeed = Integer.MIN_VALUE;

        // Execute query and process results
        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            // Extracting wind speed value from "speed" column
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "speed")) {
                String windSpeedValue = cell.getValue().toStringUtf8();
                int windSpeed = Integer.parseInt(windSpeedValue);

                // Update maximum wind speed if current value is greater
                if (windSpeed > maxWindSpeed) {
                    maxWindSpeed = windSpeed;
                }
            }
        }

        if (maxWindSpeed == Integer.MIN_VALUE) {
            System.out.println("No wind speed data found for specified query.");
            return -1; // Return -1 to indicate no data found
        }

        System.out.println("Highest wind speed in Portland in September 2022: " + maxWindSpeed);
        return maxWindSpeed;
    }

    /**
     * Query returns all readings for SeaTac for October 2, 2022. Return as an
     * ArrayList of objects arrays.
     * Each object array should have fields: date (string), hour (string),
     * temperature (int), dewpoint (int), humidity (string), windspeed (string),
     * pressure (string).
     * 
     * @return ArrayList<Object[]> containing readings.
     * @throws Exception if an error occurs.
     */
    public ArrayList<Object[]> query3() throws Exception {
        System.out.println("Executing query #3.");

        // Prefix for row keys for SeaTac on October 2, 2022
        String prefix = "seatac#2022-10-02";
        ArrayList<Object[]> data = new ArrayList<>();

        // Quering rows with prefix
        Query query = Query.create(tableId).prefix(prefix);

        // Execute query and process results
        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            String rowKey = row.getKey().toStringUtf8();
            String[] keyParts = rowKey.split("#");

            if (keyParts.length < 3)
                continue; // Skip invalid keys

            String date = keyParts[1]; // Extracting date
            String hour = keyParts[2]; // Extracting hour

            // Extracting cell values for each reading
            String temperature = getCellValue(row, "temperature");
            String dewpoint = getCellValue(row, "dewpoint");
            String humidity = getCellValue(row, "relhum");
            String windspeed = getCellValue(row, "speed");
            String pressure = getCellValue(row, "pressure");

            // data to list
            data.add(new Object[] { date, hour, Integer.parseInt(temperature), Integer.parseInt(dewpoint), humidity,
                    windspeed, pressure });
        }

        System.out.println("Query #3 completed. Retrieved " + data.size() + " readings.");
        return data;
    }

    // method to extracting cell values
    private String getCellValue(Row row, String columnQualifier) {
        for (RowCell cell : row.getCells(COLUMN_FAMILY, columnQualifier)) {
            return cell.getValue().toStringUtf8();
        }
        return "";
    }

    /**
     * Query returns highest temperature at any station in summer months of
     * 2022 (July (7), August (8)).
     * 
     * @return highest temperature as an integer.
     * @throws Exception if an error occurs.
     */
    public int query4() throws Exception {
        System.out.println("Executing query #4.");

        // Manually set start and end keys for July and August 2022
        String startKey = "portland#2022-07";
        String endKey = "portland#2022-09"; // Exclusive of September

        Query query = Query.create(tableId)
                .range(startKey, endKey);

        int maxTemp = Integer.MIN_VALUE;

        //  query and process results
        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                int temperature = Integer.parseInt(cell.getValue().toStringUtf8());
                if (temperature > maxTemp) {
                    maxTemp = temperature;
                }
            }
        }

        if (maxTemp == Integer.MIN_VALUE) {
            System.out.println("No temperature data found for specified query.");
            return -1; // Returning -1 if no data found
        }

        System.out.println("Highest temperature in summer 2022: " + maxTemp);
        return maxTemp;
    }

    /**
     * Query calculates average relative humidity for all stations on
     * 2022-10-05.
     * 
     * @return average relative humidity as an integer.
     * @throws Exception if an error occurs.
     */
    public int query5() throws Exception {
        System.out.println("Executing query #5: Calculating average relative humidity on 2022-10-05.");

        // Defining prefix for rows on specific date
        String date = "2022-10-05";
        String rowPrefix = "#" + date;

        // Building query
        Query query = Query.create(tableId)
                .prefix(rowPrefix);

        int totalHumidity = 0;
        int count = 0;

        // Executing query
        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "relhum")) {
                int humidity = Integer.parseInt(cell.getValue().toStringUtf8());
                totalHumidity += humidity;
                count++;
            }
        }

        if (count == 0) {
            System.out.println("No humidity data found for specified date: " + date);
            return -1; // Return -1 if no data is found
        }

        int avgHumidity = totalHumidity / count;
        System.out.println("Average relative humidity on " + date + ": " + avgHumidity + "%");
        return avgHumidity;
    }

    /**
     * Delete table from Bigtable.
     */
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }
}
