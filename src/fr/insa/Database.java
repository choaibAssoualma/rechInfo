package fr.insa;

import java.sql.*;
import java.util.*;

public class Database {

    /*
    *
    * 1) Inverted Index Table        : line 37
    * 2) IDF Table                   : line 118
    * 3) Requests Table               : line 180
    * 4) Requests results Table       : line 238
    * 5) Database Management methods  : line 290
    * */

    private Connection db;
    private PreparedStatement prep;
    private String dbName;
    private String invertedIndexTable;
    private String requestsTable;
    private String requestsResultsTable;
    private String idfTable;

    //initialization of the database
    public Database(String dbName) {
        this.dbName = dbName;
        this.invertedIndexTable = "InvertedIndex";
        this.requestsTable = "Requests";
        this.requestsResultsTable = "RequestsResults";
        this.idfTable = "IDF";
    }

    // 1) **************************************** Inverted Index Table ************************************************
    // Creation of the inverted index
    //WORD
    //DOCUMENT
    //FREQ : the frequency of the word on a document
    public void createInvertedIndex() {
        String sql = "CREATE TABLE " + invertedIndexTable + " " +
                "(WORD      TEXT    NOT NULL," +
                " DOCUMENT  TEXT    NOT NULL," +
                " FREQ      REAL    NOT NULL," +
                " PRIMARY KEY (WORD, DOCUMENT));";
        createTable(invertedIndexTable, sql);
    }
    // delete  inverted index
    public void deleteInvertedIndex() {
        deleteTable(invertedIndexTable);
    }

    //prepared statement to insert a record into the inverted index
    public void prepareInvertedIndexStatement() throws SQLException {
        String sql = "INSERT INTO " + invertedIndexTable + " (WORD, DOCUMENT, FREQ) VALUES(?,?,?);";
        prepareInsertStatement(sql);
    }

    //add  a record to the inverted index
    public void addToInvertedIndex(String word, String document, double frequency) throws SQLException {
        prep.setString(1, word);
        prep.setString(2, document);
        prep.setDouble(3, frequency);
        prep.addBatch();
    }

    //get the frequency of a word  in a document using the inverted index
    public int getFreq(String word, String document) throws SQLException {
        String sql = "SELECT FREQ FROM " + invertedIndexTable +
                " WHERE WORD='" + word + "' AND DOCUMENT='" + document + "';";
        return getInt(sql, "FREQ");
    }

    //get the frequency of a word  in all documents using the inverted index
    public int getAbsoluteFreq(String word) throws SQLException {
        String sql = "SELECT SUM(FREQ) AS SUM FROM " + invertedIndexTable +
                " WHERE WORD='" + word + "';";
        return getInt(sql, "SUM");
    }

    //method used in the methods getFreq/2 and getAbsoluteFreq/1 to get the frequency of a word form the inverted Index table
    private int getInt(String sql, String column) throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            return rs.getInt(column);
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }
    //Map<Document,Map<WORD,FREQUENCY of the word on the document>>
    public Map<String, Map<String, Double>> getInvertedIndex() throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            // Map with document - Map of word - freq
            Map<String, Map<String, Double>> invertedIndex = new HashMap<>();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + invertedIndexTable);
            while (rs.next()) {
                if (!invertedIndex.containsKey(rs.getString("DOCUMENT")))
                    invertedIndex.put(rs.getString("DOCUMENT"), new HashMap<>());
                invertedIndex.get(rs.getString("DOCUMENT")).put(rs.getString("WORD"), rs.getDouble("FREQ"));
            }
            return invertedIndex;
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    // 2) **************************************** IDF Table ***********************************************************
    //WORD
    //IDF: number of the documents the word is present
    public void createIDFTable() {
        String sql = "CREATE TABLE " + idfTable + " " +
                "(WORD  TEXT    PRIMARY KEY    NOT NULL," +
                " IDF   INT                    NOT NULL);";
        createTable(idfTable, sql);
    }

    public void deleteIDFTable() {
        deleteTable(idfTable);
    }

    public void prepareIDFStatement() throws SQLException {
        String sql = "INSERT INTO " + idfTable + " (WORD, IDF) VALUES(?,?);";
        prepareInsertStatement(sql);
    }

    public void addToIDFTable(String word, int frequency) throws SQLException {
        prep.setString(1, word);
        prep.setInt(2, frequency);
        prep.addBatch();
    }

    public Map<String, Integer> getIdfs() throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            // Map with word - IDF
            Map<String, Integer> idfs = new HashMap<>();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + idfTable);
            while (rs.next()) {
                idfs.put(rs.getString("WORD"), rs.getInt("IDF"));
            }
            return idfs;
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }


    //argument : a requestID  Q1,Q2..Q11
    //return : Map<WORD, WEIGHT of the word on the request (main word,synonym..etc)>

    public Map<String, Double> getRequestWords(String requestID) throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            // List of words of the request
            Map<String, Double> requestWords = new HashMap<>();
            ResultSet rs = statement.executeQuery("SELECT WORD, WEIGHT FROM " + requestsTable + " WHERE REQ_ID='" + requestID + "'");
            while (rs.next()) {
                requestWords.put(rs.getString("WORD"), rs.getDouble("WEIGHT"));
            }
            return requestWords;
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    // 3) **************************************** Requests Table ******************************************************
    //REQ_ID --> Q1 or Q2.... Q11
    //Word
    //WEIGHT  of the words & synonyms  (part 4 for the semantic search engine )
    public void createRequestsTable() {
        String sql = "CREATE TABLE " + requestsTable + " " +
                "(REQ_ID    TEXT    NOT NULL," +
                " WORD      TEXT    NOT NULL," +
                 "WEIGHT    REAL    NOT NULL,"+
                " PRIMARY KEY (REQ_ID, WORD));";
        createTable(requestsTable, sql);
    }

    public void deleteRequestsTable() {
        deleteTable(requestsTable);
    }

    public void addToRequests(String id, String word,Double weight) throws SQLException {
        if (! alreadyExists(id, word)) {
            String sql = "INSERT INTO " + requestsTable +
                    " (REQ_ID, WORD, WEIGHT) VALUES ('" + id +
                    "' , '" + word +"' , '" + weight + "')";
            executeUpdate(requestsTable, sql);
        }
    }

    private boolean alreadyExists(String id, String word) throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + requestsTable + " WHERE REQ_ID='" + id +
                                                  "' AND WORD='" + word + "';");
            return rs.next();
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    public Set<String> getRequestsID() throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            Set<String> requestsIDs = new HashSet<>();
            ResultSet rs = statement.executeQuery("SELECT REQ_ID FROM " + requestsTable + ";");
            while (rs.next()) {
                requestsIDs.add(rs.getString("REQ_ID"));
            }
            return requestsIDs;
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    // 4) **************************************** Requests results Table **********************************************
    //REQ_ID --> Q1 or Q2.... Q11
    //DOCUMENT
    //RESULT : 1 if the document is relevant for the request ,0 else
    public void createRequestsResultsTable() {
        String sql = "CREATE TABLE " + requestsResultsTable + " " +
                "(REQ_ID    TEXT    NOT NULL," +
                " DOCUMENT  TEXT    NOT NULL," +
                " RESULT    INTEGER NOT NULL," +
                " PRIMARY KEY (REQ_ID, DOCUMENT));";
        createTable(requestsResultsTable, sql);
    }

    public void deleteRequestsResultsTable() {
        deleteTable(requestsResultsTable);
    }

    public void prepareRequestsResultsStatement() throws SQLException {
        String sql = "INSERT INTO " + requestsResultsTable + " (REQ_ID, DOCUMENT, RESULT) VALUES(?,?,?);";
        prepareInsertStatement(sql);
    }

    public void addToRequestsResults(String request, String document, int result) throws SQLException {
        prep.setString(1, request);
        prep.setString(2, document);
        prep.setInt(3, result);
        prep.addBatch();
    }


    public List<String> getRequestResults(String requestID) throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            // List of pertinents documents for the request
            List<String> requestResults = new ArrayList<>();
            ResultSet rs = statement.executeQuery("SELECT DOCUMENT FROM " + requestsResultsTable + " RR " +
                    "WHERE RR.REQ_ID='" + requestID + "' AND RR.RESULT=1");
            while (rs.next()) {
                requestResults.add(rs.getString("DOCUMENT"));
            }
            return requestResults;
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    // 5) **************************************** Database Management methods ***************************************
    public void executePreparedStatement() throws SQLException {
        prep.executeBatch();
        db.commit();
        closeQuietly(db);
    }

    private void prepareInsertStatement(String sql) throws SQLException {
        openDB();
        if (this.isCreatedTable(invertedIndexTable)) {
            db.setAutoCommit(false);
            prep = db.prepareStatement(sql);
        } else {
            closeQuietly(db);
            throw new SQLException();
        }
    }

    private void createTable(String tableName, String sql) {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            if (! this.isCreatedTable(tableName)) {
                statement.executeUpdate(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    private void deleteTable(String tableName) {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            String sql = "DROP TABLE IF EXISTS " + tableName + ";";
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    private void executeUpdate(String tableName, String sql) throws SQLException {
        this.openDB();
        Statement statement = null;
        try {
            statement = this.db.createStatement();
            if (this.isCreatedTable(tableName)) {
                statement.executeUpdate(sql);
            } else {
                throw new SQLException();
            }
        } finally {
            this.closeQuietly(statement);
            this.closeQuietly(this.db);
        }
    }

    void openDB() {
        try {
            Class.forName("org.sqlite.JDBC");
            this.db = DriverManager.getConnection("jdbc:sqlite:" + this.dbName + ".db");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    boolean isCreatedTable(String tableName) {
        try {
            DatabaseMetaData md = db.getMetaData();
            ResultSet rs = md.getTables(null, null, tableName, null);
            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void closeQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
