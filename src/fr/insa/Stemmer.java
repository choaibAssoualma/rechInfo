package fr.insa;

import org.tartarus.snowball.ext.frenchStemmer;
//stemming class -we have used snowball for stemming
public class Stemmer {

    private frenchStemmer stemmer;

    public Stemmer() {
        this.stemmer = new frenchStemmer();
    }

    public String stem(String s) {
        stemmer.setCurrent(s);
        return stemmer.stem() ? stemmer.getCurrent() : s;
    }
}
