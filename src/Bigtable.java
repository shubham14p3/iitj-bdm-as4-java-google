import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
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
    public final String tableId = "weather"; // TODO: Must change table name if sharing my database
    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        BigtableAssignment bigtable = new BigtableAssignment();
        bigtable.run();
    }

    public void run() throws Exception {
        connect();
        deleteTable(); // Delete the table before creating it for fresh runs
        createTable();
        loadData();
        
        // Execute queries
        System.out.println("Temperature in Vancouver on 2022-10-01 at 10:00: " + query1());
        System.out.println("Highest windspeed in Portland during September 2022: " + query2());
        System.out.println("All readings for SeaTac on 2022-10-02: " + query3());
        System.out.println("Highest temperature in summer 2022: " + query4());

        close();
    }

    public void connect() throws IOException {
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        dataClient = BigtableDataClient.create(dataSettings);

        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);

        System.out.println("Connected to Bigtable instance.");
    }

    public void createTable() {
        try {
            CreateTableRequest request = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY);
            adminClient.createTable(request);
            System.out.println("Table created successfully.");
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
    }

    public void loadData() throws Exception {
        String path = "bin/data/";
        File folder = new File(path);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));

        if (files == null) throw new IOException("No data files found in path.");

        for (File file : files) {
            String stationId = file.getName().split("_")[0];
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length < 7) continue; // Skip invalid rows

                    String rowKey = stationId + "#" + fields[0] + "#" + fields[1]; // Unique key format
                    RowMutation mutation = RowMutation.create(tableId, rowKey)
                            .setCell(COLUMN_FAMILY, "temperature", fields[2])
                            .setCell(COLUMN_FAMILY, "dewpoint", fields[3])
                            .setCell(COLUMN_FAMILY, "humidity", fields[4])
                            .setCell(COLUMN_FAMILY, "windspeed", fields[5])
                            .setCell(COLUMN_FAMILY, "pressure", fields[6]);
                    dataClient.mutateRow(mutation);
                }
            }
        }
        System.out.println("Data loaded successfully.");
    }

    public int query1() throws Exception {
        String rowKey = "YVR#2022-10-01#10";
        Row row = dataClient.readRow(tableId, rowKey);
        return row != null ? Integer.parseInt(row.getCells(COLUMN_FAMILY, "temperature").get(0).getValue().toStringUtf8()) : -1;
    }

    public int query2() throws Exception {
        Query query = Query.create(tableId).prefix("PDX#2022-09");
        ServerStream<Row> rows = dataClient.readRows(query);

        int maxWindspeed = Integer.MIN_VALUE;
        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "windspeed")) {
                maxWindspeed = Math.max(maxWindspeed, Integer.parseInt(cell.getValue().toStringUtf8()));
            }
        }
        return maxWindspeed;
    }

    public List<Map<String, String>> query3() throws Exception {
        Query query = Query.create(tableId).prefix("SEA#2022-10-02");
        ServerStream<Row> rows = dataClient.readRows(query);

        List<Map<String, String>> results = new ArrayList<>();
        for (Row row : rows) {
            Map<String, String> entry = new HashMap<>();
            for (RowCell cell : row.getCells()) {
                entry.put(cell.getQualifier().toStringUtf8(), cell.getValue().toStringUtf8());
            }
            results.add(entry);
        }
        return results;
    }

    public int query4() throws Exception {
        Query query = Query.create(tableId).range("SEA#2022-07", "SEA#2022-09");
        ServerStream<Row> rows = dataClient.readRows(query);

        int maxTemp = Integer.MIN_VALUE;
        for (Row row : rows) {
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                maxTemp = Math.max(maxTemp, Integer.parseInt(cell.getValue().toStringUtf8()));
            }
        }
        return maxTemp;
    }

    public void deleteTable() {
        try {
            adminClient.deleteTable(tableId);
            System.out.println("Table deleted successfully.");
        } catch (NotFoundException e) {
            System.err.println("Table not found: " + e.getMessage());
        }
    }

    public void close() {
        dataClient.close();
        adminClient.close();
        System.out.println("Clients closed successfully.");
    }
}
