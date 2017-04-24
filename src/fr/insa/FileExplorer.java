package fr.insa;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileExplorer {

    public static List<String> getListOfFiles(String inputFolder) {
        List<String> filenames = new ArrayList<>();
        File folder = new File(inputFolder);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                filenames.add(file.getName());
            }
        }
        return filenames;
    }

    public static int getNumberOfFiles(String inputFolder) {
        return getListOfFiles(inputFolder).size();
    }

}
