package app_kvServer;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
/**
 *
 */
public class Store {
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
            return "";
        }
    }

    private void savePairToDisk(String key, String value) {
        File file = new File(storeDirectory, key);
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.print(value);
        } catch (IOException e) {
            System.err.println("Unable save key-value pair to disk: " + file.getAbsolutePath());
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
