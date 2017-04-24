package fr.insa;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

     /*
      *
      * 1) Documents Parsing                                    : line 55
      * 2) Requests Parsing                                    : line 169
      * 3) Requests Results Parsing                            : line 227
      * 4) Methods used inside the previous parsing methods     : line 276
      *
      */

    private int ngram;
    private Set<String> stopwords;
    private Stemmer stemmer;
    private Database db;
    private String requestsFile;
    private String requestsResultsFolder;
    private String documentsFolder;
    private int dbBatchInterval;
    private double weightNgram;
    private boolean tagWeight;
    private Map<String, Integer> idfs;


    public Parser(Database db, double weightNgram, boolean tagWeight) {
        stopwords = new HashSet<>();
        loadStopwords("in/stopwords_fr.txt");
        loadStopwords("in/stopwords_fr2.txt");
        loadStopwords("in/stopwords_fr3.txt");
        stemmer = new Stemmer();
        this.db = db;
        requestsResultsFolder = "in/qrels";
        documentsFolder = "in/corpus-utf8";
        requestsFile = "in/requetes.html";
        ngram = 2;
        idfs = new HashMap<>();
        dbBatchInterval = 1000;
        this.weightNgram = weightNgram;
        this.tagWeight = tagWeight;
    }

    // 1) **************************************** Documents Parsing ************************************************
    //parse all the documents in the folder "documentsFolder"
    //and fill IDF table
     /*Arguments
    *  none
     */
    /*Return
    *  int : number of documents
     */
    public int parseHtmlDocuments() throws SQLException {
        List<String> filenames = FileExplorer.getListOfFiles(documentsFolder);
        int numberOfDocuments = 0;    //docuemnt number
        for (String documentName : filenames) {
            System.out.println("Parsing document " + documentName);
            File input = new File(documentsFolder + "/" + documentName);
            try {
                Document doc = Jsoup.parse(input, "UTF-8");
                parseDocument(doc, documentName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            numberOfDocuments++;
        }
        System.out.println("\nFilling IDF table...");
        int i = 1;
        db.prepareIDFStatement();
        for (Map.Entry<String, Integer> wordIdf : idfs.entrySet()) {
            if (i % dbBatchInterval == 0) {
                db.executePreparedStatement();
                db.prepareIDFStatement();
            }
            db.addToIDFTable(wordIdf.getKey(), wordIdf.getValue());
            i++;
        }
        db.executePreparedStatement();
        return numberOfDocuments;
    }


    //parse One document
     /*Arguments
    *  Document
    *  Document path
     */
    /*Return
    *  none
     */
    private void parseDocument(Document doc, String filename) throws SQLException {
        List<Elements> elementsList = new ArrayList<>(); //create a list
        elementsList.add(doc.head().select("*"));  //add the head document to the list
        elementsList.add(doc.body().select("*"));  //add the body document to the list
        //map of words and their frequencies (according to html tags and distance )
        Map<String, Double> ngramFreq = cleanDoc(elementsList);
        int i = 1;
        db.prepareInvertedIndexStatement();
        //insert the document and terms on the inverted index table
        for (String ngram : ngramFreq.keySet()) {
            if (i % dbBatchInterval == 0) {
                db.executePreparedStatement();
                db.prepareInvertedIndexStatement();
            }
            double freq = ngramFreq.get(ngram);
            if (ngram.contains(" ")) freq = freq * weightNgram; // if it's n-gram...
            db.addToInvertedIndex(ngram, filename, freq);
            updateIdf(ngram);
            i++;
        }
        db.executePreparedStatement();
    }

    //compute a document terms frequency (including  html tags - and distance )
      /*Arguments
    *  List<Elements:doc head and body>
     */
    /*Return
    *  Map<a term, the term frequency in the document>
     */
    private Map<String, Double> cleanDoc(List<Elements> elementsList) {
        Map<String, Double> wordFreq = new HashMap<>();
        for (Elements elements : elementsList) {
            for (Element element : elements) {
                double score = 1;

                if (this.tagWeight) {
                    if (element.tagName().equals("title")) {
                        score = 3.15;
                    } else if (element.tagName().equals("h2") || element.tagName().equals("b")) {  //h2 is most used for important titles than h1
                        score = 3.15;
                    } else if (element.tagName().equals("h3")) {
                        score = 2;
                    } else if (element.tagName().equals("h4")) {
                        score = 1.8;
                    } else if (element.tagName().equals("h1")) {
                        score = 2.9;
                    } else if (element.tagName().equals("strong")) {
                        score = 2.95;
                    } else if (element.tagName().equals("li")) {
                        score = 1;
                    } else if (element.tagName().equals("p")) {
                        score = 1;
                    } else if (element.tagName().equals("a")) {
                        score = 1;
                    } else if (element.tagName().equals("script") || element.tagName().equals("noscript") || element.tagName().equals("img") || element.tagName().equals("link") || element.tagName().equals("meta") || element.tagName().equals("style") || element.tagName().equals("form")) {
                        score = 0;
                    }
                }

                String text = element.ownText(); //get the element text
                List<String> wordsList = clean(text); //clean the text (remove space etc..)
                List<String> ngramsList = getNGramList(wordsList);
                for (String ngram : ngramsList) {
                    if (wordFreq.containsKey(ngram)) {
                        wordFreq.replace(ngram, wordFreq.get(ngram) + score);
                    } else {
                        wordFreq.put(ngram,score);
                    }
                }
            }
        }
        return wordFreq;
    }


    public String getDocumentsFolder() {
        return this.documentsFolder;
    }

    private void updateIdf(String word) {
        if (idfs.containsKey(word)) {
            idfs.replace(word, idfs.get(word) + 1);
        } else {
            idfs.put(word, 1);
        }
    }

    // 2) **************************************** Requests Parsing ************************************************


    public void parseRequests() throws SQLException {
        File input = new File(requestsFile);
        try {
            Document doc = Jsoup.parse(input, "UTF-8");
            Elements elements = doc.body().select("*");//get only the request file body
            System.out.println("Parsing requests file");
            parseRequests(elements);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //elements : request file body
    private void parseRequests(Elements elements) throws SQLException {
        boolean lastWasMotsCles = false;
        String requestID = "";
        //for each body line
        for (Element element : elements) {
            String nodeName = element.nodeName(); //get the node name
            String ownText = element.ownText();   //get the text inside the node
            if (lastWasMotsCles && nodeName.equals("dd")) {  //if the nodename=DD "a key word or a description//
                String[] w1 = ownText.split(" #,"); //get the main part of the request
                String[] primaryFields = w1[0].split(","); //split the requests word
                for (String field : primaryFields) {
                    List<String> wordsList = clean(field);
                    List<String> ngramsList = getNGramList(wordsList);
                    for (String ngram : ngramsList) {
                        db.addToRequests(requestID, ngram, 1.0); //main words have a weight of one
                    }
                }

                List<String[]> secondaryFields = new ArrayList<>(); //get secondary  parts  of the request (synonyms & relations)
                for(int i = 1; i < w1.length; i++){
                    secondaryFields.add(w1[i].split(","));
                }

                for (String[] tsyn : secondaryFields){
                    List<String> ngramsList = new ArrayList<>();
                    for(String syn: tsyn){
                        List<String> wordsList = clean(syn);
                        ngramsList.addAll(getNGramList(wordsList));
                    }
                    int length = ngramsList.size();
                    for (String ngram : ngramsList) {
                        db.addToRequests(requestID, ngram,(1.0/3*length)); //TODO: Here, if it already exists, we can sum up the weights
                    }
                }
            }
            if (nodeName.equals("h2")) requestID = element.ownText();
            lastWasMotsCles = nodeName.equals("dt") && ownText.equals("mots clés");
        }
    }

    // 3) **************************************** Requests Results Parsing ********************************************
    public void parseRequestsResults() throws SQLException {
        List<String> filenames = FileExplorer.getListOfFiles(requestsResultsFolder);
        for (String requestID : filenames) {
            File file = new File(requestsResultsFolder + "/" + requestID);
            requestID = cleanRequestFilename(requestID);
            System.out.println("Parsing request result " + requestID);
            parseRequestResultFile(file, requestID);
        }
        System.out.println();
    }

    private String cleanRequestFilename(String filename) {
        Pattern pattern = Pattern.compile("qrel(Q\\d+)");
        Matcher matcher = pattern.matcher(filename);
        return matcher.find() ? matcher.group(1) : filename;
    }

    private void parseRequestResultFile(File file, String requestID) throws SQLException {
        try {
            Scanner inFile = new Scanner(file);
            int i = 1;
            db.prepareRequestsResultsStatement();
            while (inFile.hasNext()) {
                if (i % dbBatchInterval == 0) {
                    db.executePreparedStatement();
                    db.prepareRequestsResultsStatement();
                }
                String[] lineSplit = inFile.nextLine().split("\\t");
                String documentID = lineSplit[0];
                int isPertinent = Integer.parseInt(lineSplit[1]);
                db.addToRequestsResults(requestID, documentID, isPertinent);
                i++;
            }
            db.executePreparedStatement();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 4) ********************* methods used inside the previous parsing methods **********************
    private void loadStopwords(String filename) {
        File file = new File(filename);
        try {
            Scanner inFile = new Scanner(file);
            while (inFile.hasNext()) {
                String stopword = inFile.nextLine();
                stopwords.add(stopword);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private boolean isStopword(String word) {
        return stopwords.contains(word);
    }

    private boolean isAlpha(String word) {
        return word.matches("\\p{L}*"); // Also matches letters with accents
    }

    private List<String> getListOfWords(String[] words) {
        List<String> wordsList = new ArrayList<>();
        for (String word : words) {
            word = word.toLowerCase();
            if (isAlpha(word) && !isStopword(word) && word.length() != 0) { // TODO: Make it accept years?
                word = stemmer.stem(word);
                wordsList.add(word);
            }
        }
        return wordsList;
    }

    private List<String> clean(String text) {
        text = text.replaceAll("\\p{P}", " "); // Replace punctuation with spaces
        text = text.replaceAll("\\s+", " "); // Remove multiple spaces
        String[] words = text.split(" ");
        return getListOfWords(words); // Like this, we can use bigrams, etc in the future
    }

    private List<String> getNGramList(List<String> wordsList) {
        List<String> ngramsList = new ArrayList<>();
        for (int i = 1; i <= ngram; i++) {
            ngramsList.addAll(makeNGrams(wordsList, i));
        }
        return ngramsList;
    }

    private List<String> makeNGrams(List<String> wordsList, int n) {
        List<String> ngrams = new ArrayList<>();
        for (int i = 0; i < wordsList.size() - n + 1; i++) {
            List<String> phrase = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                phrase.add(wordsList.get(i + j));
            }
            ngrams.add(String.join(" ", phrase));
        }
        return ngrams;
    }
}
