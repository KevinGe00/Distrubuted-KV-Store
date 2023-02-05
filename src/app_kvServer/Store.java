package app_kvServer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
/**
 *
 */
public class Store {
    private static Logger logger = Logger.getRootLogger();
    private Map<String, String> store;
    private String storeDirectory;
    public Store(String storeDirectory) {
        this.store = new HashMap<>();
        this.storeDirectory = storeDirectory;
        initialize();
    }

    private void initialize() {
        File directory = new File(storeDirectory);

        if (!directory.exists()) {
            // This check needs to be first, to create non-existing directories
            directory.mkdirs();
        } else if (!directory.isDirectory()) {
            System.err.println("The directory store path must be a directory.");
            return;
        }

        for (File file : directory.listFiles()) {
            String key = file.getName();
            String value = readContent(file);
            store.put(key, value);
        }
    }

    private String readContent(File file) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            StringBuilder contents = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                contents.append(line).append("\n");
            }
            return contents.toString();
        } catch (IOException e) {
            String errMsg = "Error while reading contents of " + file.getAbsolutePath();
            System.err.println(errMsg);
            logger.error(errMsg, e);
            return "";
        }
    }

    private void savePairToDisk(String key, String value) {
        File file = new File(storeDirectory, key);
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.print(value);
        } catch (IOException e) {
            String errMsg = "Unable to save key-value pair to disk: " + file.getAbsolutePath();
            System.err.println(errMsg);
            logger.error(errMsg, e);
        }
    }
    public void put(String key, String value) {
        store.put(key, value);
        savePairToDisk(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }
}
