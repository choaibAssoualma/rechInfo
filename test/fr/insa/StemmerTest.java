package fr.insa;

import org.junit.Test;

import static org.junit.Assert.*;

public class StemmerTest {

    private Stemmer stemmer;

    public StemmerTest() {
        stemmer = new Stemmer();
    }

    @Test
    public void shouldStemOk() {
        String stemmed = stemmer.stem("intouchables");
        assertEquals("intouch", stemmed);
    }

    @Test
    public void shouldNotStem() {
        String stemmed = stemmer.stem("bonjour");
        assertEquals("bonjour", stemmed);
    }

    @Test
    public void shouldStemEvenWithAccent() {
        String stemmed = stemmer.stem("écrite");
        assertEquals("écrit", stemmed);
    }

}