package main;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Mesh_Join {

    private static final int CHUNK_SIZE = 20; // Size of each chunk to process
    private static final BlockingQueue<List<Record>> transactionsQueue = new ArrayBlockingQueue<>(5); // Queue for transaction chunks
    private static final Map<Integer, List<ProductRecord>> productPartitions = new HashMap<>(); // Product Partitions
    private static final Map<Integer, List<CustomerRecord>> customerPartitions = new HashMap<>(); // Customer Partitions
    private static final Hashtable<String, JoinedRecord> joinedRecordsTable = new Hashtable<>(); // Hashtable to store joined records
    private static final int NUMBER_OF_PARTITIONS = 5; // Number of partitions for reference data

    public static void main(String[] args) throws InterruptedException {
        // Database connection details
        String masterdata_URL = "jdbc:mysql://localhost:3306/MASTER_DATA";
        String DW_URL = "jdbc:mysql://localhost:3306/METRO_DATAWAREHOUSE";
        String dbUser = "root";
        String dbPassword = "12345678";

        // Path to the CSV file
        String customerCSV = "E:/University/Semester 5/DW&BI/Project/customers_data.csv";
        String productCSV = "E:/University/Semester 5/DW&BI/Project/products_data.csv";
        String transactionCSV = "E:/University/Semester 5/DW&BI/Project/transactions.csv";

        CreateMasterData masterData = new CreateMasterData(masterdata_URL, dbUser, dbPassword);

        masterData.fillCustomerTable(customerCSV);
        masterData.fillProductTable(productCSV);

        masterData.loadProductDataPartitions(productPartitions, NUMBER_OF_PARTITIONS);
        masterData.loadCustomerDataPartitions(customerPartitions, NUMBER_OF_PARTITIONS);

        Thread transactionReaderThread = new Thread(() -> masterData.loadTransactionData(transactionCSV));
        transactionReaderThread.start();

        ExecutorService executor = Executors.newFixedThreadPool(3);
        executor.submit(Mesh_Join::performMeshJoin);

        transactionReaderThread.join();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        insertJoinedRecordsIntoDW(DW_URL, dbUser, dbPassword);
    }

    // Record class to hold transaction data
    public static class Record {
        String orderId, orderDate, productId, customerId, timeId;
        int quantity;
        double salesRevenue;

        public Record(String orderId, String orderDate, String productId, int quantity, String customerId,
                String timeId) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.productId = productId;
            this.quantity = quantity;
            this.customerId = customerId;
            this.timeId = timeId;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "orderId='" + orderId + '\'' +
                    ", productId='" + productId + '\'' +
                    ", customerId='" + customerId + '\'' +
                    '}';
        }
    }

    // Class to store joined records for the DW
    public static class JoinedRecord {
        String orderId, productId, customerId, timeId, storeId, supplierId, orderDate, productName, customerName,
                gender, storeName, supplierName;
        double price, salesRevenue;
        int quantity;

        public JoinedRecord(String orderId, String productId, String customerId, String timeId, String storeId,
                String supplierId,
                String orderDate, String productName, String customerName, String gender, String storeName,
                String supplierName,
                double price, int quantity, double salesRevenue) {
            this.orderId = orderId;
            this.productId = productId;
            this.customerId = customerId;
            this.timeId = timeId;
            this.storeId = storeId;
            this.supplierId = supplierId;
            this.orderDate = orderDate;
            this.productName = productName;
            this.customerName = customerName;
            this.gender = gender;
            this.storeName = storeName;
            this.supplierName = supplierName;
            this.price = price;
            this.quantity = quantity;
            this.salesRevenue = salesRevenue;
        }

        @Override
        public String toString() {
            return "JoinedRecord{" +
                    "orderId='" + orderId + '\'' +
                    ", productId='" + productId + '\'' +
                    ", customerId='" + customerId + '\'' +
                    ", salesRevenue=" + salesRevenue +
                    '}';
        }
    }

    // Product record class to hold product data
    public static class ProductRecord {
        String productId, productName, supplierId, supplierName, storeId, storeName;
        double price;

        public ProductRecord(String productId, String productName, double price, String supplierId, String supplierName,
                String storeId, String storeName) {
            this.productId = productId;
            this.productName = productName;
            this.price = price;
            this.supplierId = supplierId;
            this.supplierName = supplierName;
            this.storeId = storeId;
            this.storeName = storeName;
        }

        @Override
        public String toString() {
            return "ProductRecord{" +
                    "productId='" + productId + '\'' +
                    ", productName='" + productName + '\'' +
                    '}';
        }
    }

    // Customer record class to hold customer data
    public static class CustomerRecord {
        String customerId, customerName, gender;

        public CustomerRecord(String customerId, String customerName, String gender) {
            this.customerId = customerId;
            this.customerName = customerName;
            this.gender = gender;
        }

        @Override
        public String toString() {
            return "CustomerRecord{" +
                    "customerId='" + customerId + '\'' +
                    ", customerName='" + customerName + '\'' +
                    ", Gender='" + gender + '\'' +
                    '}';
        }
    }

    private static void performMeshJoin() {
        try {
            while (true) {
                List<Record> transactionsChunk = transactionsQueue.take();
                if (transactionsChunk.isEmpty())
                    break; // No more data to process

                for (Record transaction : transactionsChunk) {
                    int PID = Integer.parseInt(transaction.productId) - 1;
                    int CID = Integer.parseInt(transaction.customerId) - 1;
                    int productPartitionNumber = (PID) % NUMBER_OF_PARTITIONS;
                    int customerPartitionNumber = (CID) % NUMBER_OF_PARTITIONS;
                    List<ProductRecord> productPartition = productPartitions.get(productPartitionNumber);
                    List<CustomerRecord> customerPartition = customerPartitions.get(customerPartitionNumber);

                    ProductRecord productRecord = null;
                    CustomerRecord customerRecord = null;

                    for (ProductRecord tempProductRecord : productPartition) {
                        if (transaction.productId.equals(tempProductRecord.productId)) {
                            productRecord = tempProductRecord;
                        }

                    }

                    for (CustomerRecord tempCustomerRecord : customerPartition) {

                        if (transaction.customerId.equals(tempCustomerRecord.customerId)) {
                            customerRecord = tempCustomerRecord;
                        }

                    }

                    if (transaction.productId.equals(productRecord.productId)
                            && transaction.customerId.equals(customerRecord.customerId)) {
                        double salesRevenue = transaction.quantity * productRecord.price;

                        // Create joined record
                        JoinedRecord joinedRecord = new JoinedRecord(
                                transaction.orderId,
                                transaction.productId,
                                transaction.customerId,
                                transaction.timeId,
                                productRecord.storeId,
                                productRecord.supplierId,
                                transaction.orderDate,
                                productRecord.productName,
                                customerRecord.customerName,
                                customerRecord.gender,
                                productRecord.supplierName,
                                productRecord.storeName,
                                productRecord.price,
                                transaction.quantity,
                                salesRevenue);

                        // Insert into the hashtable
                        joinedRecordsTable.put(transaction.orderId, joinedRecord);
                        System.out.printf("Joined Transaction: %s + %s + %s%n", transaction, productRecord,
                                customerRecord);

                    }
                }

            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    private static void insertJoinedRecordsIntoDW(String DW_URL, String dbUser, String dbPassword) {
        try (Connection dwConnection = DriverManager.getConnection(DW_URL, dbUser, dbPassword)) {
            String insertSalesSQL = "INSERT INTO sales (order_id, product_id, customer_id, supplier_id, time_id, store_id, quantity, sales_revenue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            String insertProductSQL = "INSERT IGNORE INTO product (product_id, product_name, price) VALUES (?, ?, ?)";
            String insertCustomerSQL = "INSERT IGNORE INTO customer (customer_id, customer_name, gender) VALUES (?, ?, ?)";
            String insertStoreSQL = "INSERT IGNORE INTO store (store_id, store_name) VALUES (?, ?)";
            String insertSupplierSQL = "INSERT IGNORE INTO supplier (supplier_id, supplier_name) VALUES (?, ?)";
            String insertDateSQL = "INSERT IGNORE INTO date (time_id, date_t, time_t, weekend, half_of_year, month_t, quarter_t, year_t) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            dwConnection.setAutoCommit(false);

            SimpleDateFormat orderDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
            SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss", Locale.ENGLISH);

            try (PreparedStatement preparedStatement = dwConnection.prepareStatement(insertSalesSQL);
                    PreparedStatement productStmt = dwConnection.prepareStatement(insertProductSQL);
                    PreparedStatement customerStmt = dwConnection.prepareStatement(insertCustomerSQL);
                    PreparedStatement storeStmt = dwConnection.prepareStatement(insertStoreSQL);
                    PreparedStatement supplierStmt = dwConnection.prepareStatement(insertSupplierSQL);
                    PreparedStatement dateStmt = dwConnection.prepareStatement(insertDateSQL)) {

                for (JoinedRecord joinedRecord : joinedRecordsTable.values()) {

                    // Insert product
                    productStmt.setInt(1, Integer.parseInt(joinedRecord.productId));
                    productStmt.setString(2, joinedRecord.productName);
                    productStmt.setDouble(3, joinedRecord.price);
                    productStmt.addBatch();

                    // Insert customer
                    customerStmt.setInt(1, Integer.parseInt(joinedRecord.customerId));
                    customerStmt.setString(2, joinedRecord.customerName);
                    customerStmt.setString(3, joinedRecord.gender);
                    customerStmt.addBatch();

                    // Insert store
                    storeStmt.setInt(1, Integer.parseInt(joinedRecord.storeId));
                    storeStmt.setString(2, joinedRecord.storeName);
                    storeStmt.addBatch();

                    // Insert supplier
                    supplierStmt.setInt(1, Integer.parseInt(joinedRecord.supplierId));
                    supplierStmt.setString(2, joinedRecord.supplierName);
                    supplierStmt.addBatch();

                    try {

                        java.util.Date parsedDate = orderDateFormat.parse(joinedRecord.orderDate);
                        String datePart = dateFormat.format(parsedDate);
                        String timePart = timeFormat.format(parsedDate);

                        // Extract additional information using Calendar
                        java.util.Calendar calendar = java.util.Calendar.getInstance();
                        calendar.setTime(parsedDate);
                        int year = calendar.get(java.util.Calendar.YEAR);
                        int month = calendar.get(java.util.Calendar.MONTH) + 1;
                        int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
                        boolean isWeekend = (dayOfWeek == java.util.Calendar.SATURDAY
                                || dayOfWeek == java.util.Calendar.SUNDAY);
                        int quarter = (month - 1) / 3 + 1;
                        int halfOfYear = (month <= 6) ? 1 : 2;

                        // Insert date record
                        dateStmt.setInt(1, Integer.parseInt(joinedRecord.orderId));
                        dateStmt.setDate(2, java.sql.Date.valueOf(datePart));
                        dateStmt.setTime(3, java.sql.Time.valueOf(timePart));
                        dateStmt.setBoolean(4, isWeekend);
                        dateStmt.setInt(5, halfOfYear);
                        dateStmt.setInt(6, month);
                        dateStmt.setInt(7, quarter);
                        dateStmt.setInt(8, year);
                        dateStmt.addBatch();

                    } catch (Exception e) {
                        System.err.println(
                                "Error parsing or preparing date insert for order_id: " + joinedRecord.orderId);
                        e.printStackTrace();
                    }

                    // insert sales
                    preparedStatement.setInt(1, Integer.parseInt(joinedRecord.orderId));
                    preparedStatement.setInt(2, Integer.parseInt(joinedRecord.productId));
                    preparedStatement.setInt(3, Integer.parseInt(joinedRecord.customerId));
                    preparedStatement.setInt(4, Integer.parseInt(joinedRecord.supplierId));
                    preparedStatement.setInt(5, Integer.parseInt(joinedRecord.orderId));
                    preparedStatement.setInt(6, Integer.parseInt(joinedRecord.storeId));
                    preparedStatement.setInt(7, joinedRecord.quantity);
                    preparedStatement.setDouble(8, joinedRecord.salesRevenue);
                    preparedStatement.addBatch();
                }
                // Execute the batch
                productStmt.executeBatch();
                customerStmt.executeBatch();
                storeStmt.executeBatch();
                supplierStmt.executeBatch();
                dateStmt.executeBatch();
                preparedStatement.executeBatch();

                dwConnection.commit();
                System.out.println("All joined records have been inserted into the Data Warehouse.");

            } catch (SQLException e) {
                dwConnection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static class CreateMasterData {
        private String jdbcURL;
        private String dbUser;
        private String dbPassword;

        public CreateMasterData(String jdbcURL, String dbUser, String dbPassword) {
            this.jdbcURL = jdbcURL;
            this.dbUser = dbUser;
            this.dbPassword = dbPassword;
        }

        // Method to load transaction data from CSV in chunks
        public void loadTransactionData(String csvFilePath) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
                String line;
                List<Record> chunk = new ArrayList<>();
                int count = 0;

                br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    Record record = new Record(parts[0], parts[1], parts[2], Integer.parseInt(parts[3]), parts[4],
                            parts[5]);
                    chunk.add(record);
                    count++;

                    if (count == CHUNK_SIZE) {
                        transactionsQueue.put(chunk);
                        chunk = new ArrayList<>();
                        count = 0;
                    }

                }

                // Add any remaining records to the queue
                if (!chunk.isEmpty()) {
                    transactionsQueue.put(chunk);
                }

                // Signal the end of data
                transactionsQueue.put(Collections.emptyList());

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Method to load product data from database into partitions
        public void loadProductDataPartitions(Map<Integer, List<ProductRecord>> productPartitions,
                int numberOfPartitions) {
            try (Connection connection = DriverManager.getConnection(jdbcURL, dbUser, dbPassword)) {
                String query = "SELECT * FROM MASTER_DATA.product"; // Replace with your actual SQL query for product
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet rs = statement.executeQuery();

                int currentPartition = 0;
                while (rs.next()) {
                    ProductRecord record = new ProductRecord(
                            rs.getString("product_id"),
                            rs.getString("product_name"),
                            rs.getDouble("price"),
                            rs.getString("supplier_id"),
                            rs.getString("supplier_name"),
                            rs.getString("store_id"),
                            rs.getString("store_name"));

                    // Add product record to the current partition
                    productPartitions.putIfAbsent(currentPartition, new ArrayList<>());
                    productPartitions.get(currentPartition).add(record);

                    // Rotate partition for next record
                    currentPartition = (currentPartition + 1) % numberOfPartitions;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Method to load customer data from database into partitions
        public void loadCustomerDataPartitions(Map<Integer, List<CustomerRecord>> customerPartitions,
                int numberOfPartitions) {
            try (Connection connection = DriverManager.getConnection(jdbcURL, dbUser, dbPassword)) {
                String query = "SELECT * FROM MASTER_DATA.customer"; // Replace with your actual SQL query for customer
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet rs = statement.executeQuery();

                int currentPartition = 0;
                while (rs.next()) {
                    CustomerRecord record = new CustomerRecord(
                            rs.getString("customer_id"),
                            rs.getString("customer_name"),
                            rs.getString("gender"));

                    // Add customer record to the current partition
                    customerPartitions.putIfAbsent(currentPartition, new ArrayList<>());
                    customerPartitions.get(currentPartition).add(record);

                    // Rotate partition for next record
                    currentPartition = (currentPartition + 1) % numberOfPartitions;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void fillCustomerTable(String customerCSV) {
            String sqlInsert = "INSERT INTO customer (customer_id, customer_name, gender) VALUES (?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(jdbcURL, dbUser, dbPassword)) {
                connection.setAutoCommit(false); // For transaction management

                try (BufferedReader br = new BufferedReader(new FileReader(customerCSV));
                        PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert)) {

                    String line;
                    int batchSize = 10;
                    int count = 0;

                    br.readLine();

                    while ((line = br.readLine()) != null) {
                        String[] data = line.split(",");
                        int customerId = Integer.parseInt(data[0].trim());
                        String customerName = data[1].trim();
                        String gender = data[2].trim();

                        preparedStatement.setInt(1, customerId);
                        preparedStatement.setString(2, customerName);
                        preparedStatement.setString(3, gender);

                        preparedStatement.addBatch();

                        if (++count % batchSize == 0) {
                            preparedStatement.executeBatch();
                        }
                    }

                    preparedStatement.executeBatch();
                    connection.commit();
                    System.out.println("Data has been inserted successfully!");

                } catch (IOException | SQLException e) {
                    connection.rollback();
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void fillProductTable(String productFilePath) {
            String sqlInsert = "INSERT INTO product (product_id, product_name, price, supplier_id, supplier_name, store_id, store_name) VALUES (?, ?, ?, ?, ?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(jdbcURL, dbUser, dbPassword)) {
                connection.setAutoCommit(false);

                try (BufferedReader br = new BufferedReader(new FileReader(productFilePath));
                        PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert)) {

                    String line;
                    int batchSize = 10;
                    int count = 0;

                    br.readLine();

                    while ((line = br.readLine()) != null) {
                        String[] data = line.split(",");

                        String productId;
                        String productName;
                        String price;
                        String supplierId;
                        String supplierName;
                        String storeId;
                        String storeName;

                        if (isInteger(data[5])) {
                            productId = data[0].trim();
                            productName = data[1].trim();
                            price = data[2].trim();
                            supplierId = data[3].trim();
                            supplierName = data[4].trim();
                            storeId = data[5].trim();
                            storeName = data[6].trim();
                        } else {
                            productId = data[0].trim();
                            productName = data[1].trim();
                            price = data[2].trim();
                            supplierId = data[3].trim();
                            supplierName = data[4].trim() + data[5].trim();
                            storeId = data[6].trim();
                            storeName = data[7].trim();
                        }

                        int productIdInt = Integer.parseInt(productId);
                        int supplierIdInt = Integer.parseInt(supplierId);
                        int storeIdInt = Integer.parseInt(storeId);
                        double priceDouble = Double.parseDouble(price.replaceAll("[^\\d.]", ""));

                        preparedStatement.setInt(1, productIdInt);
                        preparedStatement.setString(2, productName);
                        preparedStatement.setDouble(3, priceDouble);
                        preparedStatement.setInt(4, supplierIdInt);
                        preparedStatement.setString(5, supplierName);
                        preparedStatement.setInt(6, storeIdInt);
                        preparedStatement.setString(7, storeName);

                        preparedStatement.addBatch();

                        if (++count % batchSize == 0) {
                            preparedStatement.executeBatch();
                        }
                    }

                    preparedStatement.executeBatch();
                    connection.commit();
                    System.out.println("Data has been inserted successfully!");

                } catch (IOException | SQLException e) {
                    connection.rollback();
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        private static boolean isInteger(String str) {
            try {
                Integer.parseInt(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }
}