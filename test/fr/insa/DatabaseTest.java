package fr.insa;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class DatabaseTest {

    private Database db;

    @Before
    public void setUp() {
        db = new Database("test");
    }

    @After
    public void tearDown() {
        File file = new File("test.db");
        file.delete();
    }

    @Test
    public void shouldCreateDatabase() {
        File file = new File("test.db");
        assertFalse(file.exists());
        db.openDB();
        assertTrue(file.exists());
    }

    @Test
    public void shouldNotExistDocumentTable() {
        db.openDB();
        boolean returnValue = db.isCreatedTable("D1");
        assertFalse(returnValue);
    }

    @Test
    public void shouldCreateDocumentTable() {
        db.createInvertedIndex();
        db.openDB();
        boolean returnValue = db.isCreatedTable("InvertedIndex");
        assertTrue(returnValue);
    }

    @Test
    public void shouldInsertWordDocument() throws SQLException {
        db.createInvertedIndex();
        db.addToInvertedIndex("hello", "D1", 2);
        assertEquals(2, db.getFreq("hello", "D1"));
    }

    @Test(expected=SQLException.class)
    public void shouldNotBeAbleToDuplicateWordInSameDocument() throws SQLException {
        db.createInvertedIndex();
        db.addToInvertedIndex("hello", "D1", 2);
        db.addToInvertedIndex("hello", "D1", 3);
    }

    @Test
    public void shouldBeAbleToDuplicateWordInDifferentDocument() throws SQLException {
        db.createInvertedIndex();
        db.addToInvertedIndex("hello", "D1", 3);
        db.addToInvertedIndex("hello", "D2", 2);
        assertEquals(3, db.getFreq("hello", "D1"));
        assertEquals(2, db.getFreq("hello", "D2"));
    }

    @Test(expected=SQLException.class)
    public void shouldFailGettingAmountInexistentWord() throws SQLException {
        db.createInvertedIndex();
        db.getFreq("hello", "D1");
    }

    @Test
    public void shoulGetFreqTotal() throws SQLException {
        db.createInvertedIndex();
        db.addToInvertedIndex("hello", "D1", 2);
        db.addToInvertedIndex("hello", "D2", 3);
        assertEquals(5, db.getAbsoluteFreq("hello"));
    }
}