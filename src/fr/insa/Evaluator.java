package fr.insa;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Evaluator {

    private Database db;
    private Map<String, Map<String, Double>> invertedIndex; //<Document,Map<Word,theWordFrequency>>
    private Map<String, Integer> idfs;  //String : a  word w ; Integer : number of documents containing w
    private Map<String, Double> norms; //String : a document D ; Double : ||D||=sum(tfidf^2)

    private double numberOfDocuments;
    private boolean tfNormalized;
    private boolean cosinus;
    private boolean jaccard;
    private boolean dice;
    private boolean idfRequest;
    private boolean synonymsWeights;
    private boolean tfMultiply;
    private boolean tfLog;
    private boolean tfDiv;

    public Evaluator(Database db, double numberOfDocuments, boolean tfNormalized, boolean cosinus, boolean jaccard, boolean dice, boolean idfRequest, boolean tfMultiply, boolean tfDiv, boolean tfLog, boolean synonymsWeights) {
        this.db = db;
        this.tfNormalized = tfNormalized;
        this.numberOfDocuments = numberOfDocuments;
        this.cosinus = cosinus;
        this.jaccard = jaccard;
        this.dice = dice;
        this.idfRequest = idfRequest;
        this.tfMultiply = tfMultiply;
        this.synonymsWeights = synonymsWeights;
        this.tfLog = tfLog;
        this.tfDiv = tfDiv;
    }

    public void initialize() throws SQLException{
        invertedIndex = db.getInvertedIndex();
        idfs = db.getIdfs();
        updateInvertedIndexWithTFIDFAndCalculateNorms();
    }

    /*Arguments
     *  atResult : 5,10 or 25
     */
    /*Return
     *  requestPrecision<Request_ID :Q1..Q11,number of pertinent document>
     */
    public Map<String, Integer> getNumberOfPertinentDocuments(int atResult) throws SQLException {
        Map<String, Integer> requestPrecision = new HashMap<>();
        for (String requestID : db.getRequestsID()) {
            Map<String, Double> requestWords = db.getRequestWords(requestID);
            List<String> requestResults = db.getRequestResults(requestID);
            Map<String, Double> results = generateResults(requestWords);
            results = calculateSimilarity(results, requestWords);
            requestPrecision.put(requestID, getNumberOfPertinentDocuments(results, requestResults, atResult));
        }
        return requestPrecision;
    }

    /*Arguments
     *  results<Document D, (Q&D) Similarity sorted by value>
     *  requestResults<Relevant document for this request according to user>
     *  atResult : 5,10 or 25
     */
    /*Return
     *  number of pertinent documents (our result inter user result)
     */
    private Integer getNumberOfPertinentDocuments(Map<String, Double> results, List<String> requestResults, int atResult) {
        int i = 0;
        int count = 0;
        for (Map.Entry<String, Double> document : results.entrySet()) {
            if (i == atResult) break;
            if (requestResults.contains(document.getKey())) count++;
            i++;
        }
        return count;
    }

    /*Arguments
     *  results<Document D, D inter Q>
     *  requestWords<WORD, WEIGHT of the word on the request (main word,synonym..etc)>
     */
    /*Return
     * results<Document D, Similarity sorted by value>
     */
    private Map<String, Double> calculateSimilarity(Map<String, Double> results, Map<String, Double> requestWords) {
        double normRequest = calculateRequestNorm(requestWords);
        if (cosinus) {
            for (Map.Entry<String, Double> document : results.entrySet()) {
                document.setValue(document.getValue() / Math.sqrt(norms.get(document.getKey()) * normRequest));
            }
        } else if (jaccard) {
            for (Map.Entry<String, Double> document : results.entrySet()) {
                document.setValue(document.getValue() / (norms.get(document.getKey()) + normRequest - document.getValue()));
            }
        } else if (dice) {
            for (Map.Entry<String, Double> document : results.entrySet()) {
                document.setValue(2.0 * document.getValue() / (norms.get(document.getKey()) + normRequest));
            }
        }
        return sortByValue(results);
    }

    /*Arguments
     *  Map<WORD, WEIGHT of the word on the request (main word,synonym..etc)>
     */
    /*Return
     *  the norm of the request ||Q||
     */
    private double calculateRequestNorm(Map<String, Double> requestWords) {
        double norm = 0.0;
        for (Map.Entry<String, Double> wordWeight : requestWords.entrySet()) {
            String word = wordWeight.getKey();
            Double weight = wordWeight.getValue();

            Double value = this.idfRequest ? 1.0 * idfs.getOrDefault(word, 0) : 1.0;
            if (synonymsWeights)
                value = value * weight;

            norm += Math.pow(value, 2);
        }
        return norm;
    }

    /*Arguments
     *  Map<WORD, WEIGHT of the word on the request (main word,synonym..etc)>
     */
    /*Return
     *  results<Document D, D inter Q(similarité par produit scalaire)>
     */
    private Map<String, Double> generateResults(Map<String, Double> requestWords) {
        // Map with document - result for the request
        Map<String, Double> results = new HashMap<>();
        for (Map.Entry<String, Double> wordWeight : requestWords.entrySet()) {
            for (Map.Entry<String, Map<String, Double>> entry : invertedIndex.entrySet()) {
                String word = wordWeight.getKey();
                Double weight = wordWeight.getValue();

                String document = entry.getKey();
                Map<String, Double> wordsFreqs = entry.getValue();
                double sum = wordsFreqs.getOrDefault(word, 0.0);
                if (synonymsWeights)
                    sum = sum * weight;
                if (this.idfRequest && idfs.containsKey(word))
                    sum = sum * idfs.get(word);
                if (results.containsKey(document))
                    results.replace(document, results.get(document) + sum);
                else results.put(document, sum);
            }
        }
        return results;
    }

    /* calculate tfidf
     *    update inverted index
     *    calculate  foreach document D his norm ||D||=sum(tfidf^2) and place the result on norms map
     */
    private void updateInvertedIndexWithTFIDFAndCalculateNorms() {
        // Map with document - norm
        norms = new HashMap<>();
        for (Map.Entry<String, Map<String, Double>> entry : invertedIndex.entrySet()) {
            String document = entry.getKey();
            norms.put(document, 0.0);

            Map<String, Double> wordsFreqs = entry.getValue();
            double normalization = tfNormalized ? Collections.max(wordsFreqs.values()) : 1.0;
            for (Map.Entry<String, Double> wordFreq : wordsFreqs.entrySet()) {
                // Appliance of TF-IDF...
                double tf = 0.0;
                if(tfMultiply){
                      tf = wordFreq.getValue() * normalization;
                }
                else if(tfDiv) {
                     tf = wordFreq.getValue() / normalization;
                }
                else if (tfLog) {
                     tf = 1+ (Math.log(wordFreq.getValue()) / Math.log(10) * normalization);
                }
                double idf = Math.log(this.numberOfDocuments / idfs.get(wordFreq.getKey()));
                double tfidf = tf * idf;
                wordFreq.setValue(tfidf);
                norms.replace(document, norms.get(document) + Math.pow(tfidf, 2));
            }
            norms.replace(document, Math.sqrt(norms.get(document)));
        }
    }

    /*Arguments
     *   atPrecision 5,10 or 25
     *  result<Request_ID :Q1..Q11,number of pertinent document>
     */
    /*Return
     *
     */
    public void printPrecision(int atPrecision, Map<String, Integer> result) {
        System.out.println("P@" + atPrecision + ":");
        double precisionAcum = 0;
        for (Map.Entry<String, Integer> requestNumberOfPertinents : result.entrySet()) {
            double precision = requestNumberOfPertinents.getValue() / (double) atPrecision;
            System.out.println("\tREQUEST " + requestNumberOfPertinents.getKey() + " = " + precision);
            precisionAcum += precision;
        }
        double meanPrecision = precisionAcum/result.size();
        System.out.println("\n\tMean P@" + atPrecision + "= " + meanPrecision + "\n");
    }

    /*Arguments
     *   atRecall 5,10 or 25
     *  result<Request_ID :Q1..Q11,number of pertinent document>
     */
    /*Return
     *
     */
    public void printRecall(int atRecall, Map<String, Integer> result) throws SQLException {
        System.out.println("R@" + atRecall + ":");
        double recallAcum = 0;
        for (Map.Entry<String, Integer> requestNumberOfPertinents : result.entrySet()) {
            double recall = requestNumberOfPertinents.getValue() / (double) db.getRequestResults(requestNumberOfPertinents.getKey()).size();
            System.out.println("\tREQUEST " + requestNumberOfPertinents.getKey() + " = " + recall);
            recallAcum += recall;
        }
        double meanPrecision = recallAcum/result.size();
        System.out.println("\n\tMean R@" + atRecall + "= " + meanPrecision + "\n");
    }

    /*Arguments
     *   double[] recalls = {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
     */
    /*Return
     *
     */
    public void getInterpolatedPrecisionRecallCurvePoints(double[] recalls) throws SQLException {
        System.out.println("==================================================== Precision-Recall Curve Points ====================================================");
        for (String requestID : db.getRequestsID()) {
            Map<String, Double> requestWords = db.getRequestWords(requestID);
            List<String> requestResults = db.getRequestResults(requestID);
            Map<String, Double> results = generateResults(requestWords);
            results = calculateSimilarity(results, requestWords);

            int amountOfPertinentResults = requestResults.size();
            System.out.println("\n---------- REQUEST " + requestID + " ----------");
            for (double recall : recalls) {
                double amountDocumentsToFind = amountOfPertinentResults * recall;
                double precision = getPrecisionForRecall(amountDocumentsToFind, results, requestResults);
                System.out.println(precision);
            }
        }
    }

    private double getPrecisionForRecall(double amountDocumentsToFind, Map<String, Double> results, List<String> requestResults) {
        double i = 0.0;
        int amountDocumentsFound = 0;
        double maxPrecision = 0.0;
        for (Map.Entry<String, Double> document : results.entrySet()) {
            if (requestResults.contains(document.getKey())) amountDocumentsFound++;
            i++;
            double precision = amountDocumentsFound/i;
            if (amountDocumentsFound >= amountDocumentsToFind && maxPrecision < precision) maxPrecision = precision;
        }
        return maxPrecision;
    }

    /*Arguments
     *  no Sorted Map<String, Double>
     */
    /*Return
     *  Sorted by value Map<String, Double>
     */
    private Map<String, Double> sortByValue(Map<String, Double> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}
