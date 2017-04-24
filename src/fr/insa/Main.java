package fr.insa;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {

    private static boolean fillDBWithDocuments = true;
    private static boolean fillDBWithRequests = true;
    private static boolean fillDBWithRequestsResults = true;

    // If change this, rerun with fillDBWithDocuments = true, in order to "apply" the changes
    // This gives the ngram a weight in relation to a "normal" word, when making
    private static double ngramWeight = 1.1; // 0.0 = no bigrams

    // Ways of calculating TF (if all false it's only term frequency)
    private static boolean tfNormalized = true;

    // By error, we have been doing the normalization with * instead of /, but with * it gives better results
    // So, this boolean is to change easily between them in order to facilitate the testing
    // Warning: Only one of the next ones must be in true
    private static boolean tfMultiply = false;
    private static boolean tfDiv = false;
    private static boolean tfLog = true;

    // Ways of calculating IDF (if all false it's IDF = log(#docs/frequency of that term in documents)
    // For the moment there is nothing but "normal" IDF ^

    // Use idf also in the request vector (And not only 1/0 for present/absent)
    private static boolean idfRequest = false;

    // Ways of calculating the similarity between documents and queries
    // Warning: Only one of the next ones must be in true (or none for scalar product...)
    private static boolean cosinus = true;
    private static boolean jaccard = false;
    private static boolean dice = false;

    // Using weight for the synonyms/relations
    private static boolean synonymsWeights = true;

    // If change this, rerun with fillDBWithDocuments = true, in order to "apply" the changes
    // Using weights for the tags
    private static boolean tagsWeights = true;


    public static void main(String[] args) {
        Database db = new Database("database");
        Parser parser = new Parser(db, ngramWeight, tagsWeights);

        // Construction
        long startTime = System.currentTimeMillis();
        double numberOfDocuments = 0;
        try {
            if (fillDBWithDocuments) {
                db.deleteInvertedIndex();
                db.createInvertedIndex();
                db.deleteIDFTable();
                db.createIDFTable();
                numberOfDocuments = parser.parseHtmlDocuments();
                printElapsedTime(startTime);
            } else {
                numberOfDocuments = FileExplorer.getNumberOfFiles(parser.getDocumentsFolder());
            }
            if (fillDBWithRequests) {
                db.deleteRequestsTable();
                db.createRequestsTable();
                parser.parseRequests();
                printElapsedTime(startTime);
            }
            if (fillDBWithRequestsResults) {
                db.deleteRequestsResultsTable();
                db.createRequestsResultsTable();
                parser.parseRequestsResults();
                printElapsedTime(startTime);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Usage/Evaluation
        int[] atResults = {5, 10, 25};
        double[] recalls = {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
        try {
            Evaluator evaluator = new Evaluator(db, numberOfDocuments, tfNormalized, cosinus, jaccard, dice, idfRequest, tfMultiply, tfDiv, tfLog, synonymsWeights);
            evaluator.initialize();
            for (int atResult : atResults) {
                Map<String, Integer> result = evaluator.getNumberOfPertinentDocuments(atResult);
                evaluator.printPrecision(atResult, result);
                evaluator.printRecall(atResult, result);
            }
            evaluator.getInterpolatedPrecisionRecallCurvePoints(recalls);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private static void printElapsedTime(long startTime) {
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(elapsedTime),
                TimeUnit.MILLISECONDS.toMinutes(elapsedTime) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(elapsedTime) % TimeUnit.MINUTES.toSeconds(1));
        System.out.println("Elapsed time : " + elapsedTime + " ms = " + hms);
        System.out.println("############################################################");
    }
}
