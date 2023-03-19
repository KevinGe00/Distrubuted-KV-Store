package app_kvServer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Store {
    private static Logger logger = Logger.getRootLogger();

    private String dirStore;
    private Map<String, String> store;
    private Map<String, ReadWriteLock> fileLocks;
    
    /**
     * Initialize Store which is facilitates the persistence mechanisms
     * User must catch exception from this class.
     * @param dirStore relative of where the data should be persisted
     */
    public Store(String dirStore) throws Exception {
        this.dirStore = dirStore + File.separator + "Coordinator";
        store = new HashMap<>();
        fileLocks = new HashMap<>();

        initialize();
    }

    private void initialize() throws Exception {
        File directory = new File(dirStore);

        if (!directory.exists()) {
            // This check needs to be first, to create non-existing directories
            logger.info("Store directory does not exist, creating directory for Store...");
            directory.mkdirs();
        } else if (!directory.isDirectory()) {
            System.err.println("The directory store path must be a directory.");
            throw new Exception("Invalid directory input for server Store.");
        }

        String infoMsg = "KV pairs already in disk:";
        for (File file : directory.listFiles()) {
            String key = file.getName();
            String value = readContent(file);
            store.put(key, value);
            fileLocks.put(key, new ReentrantReadWriteLock());
            infoMsg = infoMsg + " <" + key + ">";
        }
        logger.info(infoMsg);
    }

    public String readContent(File file) {
        // one-time call during initialization, no need for file lock
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

    /* File-lock-related methods */
    /**
     * Get/Create the write lock for a key from the HashMap.
     * @param key the key
     * @return a write lock
     */
    private Lock getKeyWriteLock(String key) {
        ReadWriteLock rwLock = fileLocks.get(key);
        if (rwLock == null) {
            logger.debug("Created a new read/write lock for key <" + key + ">.");
            fileLocks.put(key, new ReentrantReadWriteLock());
            rwLock = fileLocks.get(key);
        }
        return rwLock.writeLock();
    }
    /**
     * Get the shared read lock for a key from the HashMap.
     * For READ, ensure the key exists before calling this method.
     * @param key the key
     * @return a shared read lock
     */
    private Lock getKeyReadLock(String key) throws Exception {
        ReadWriteLock rwLock = fileLocks.get(key);
        if (rwLock == null) {
            logger.error("Did NOT check if the key <" + key + "> existed. "
                        + "Tried to get a READ lock for the non-existing key.");
            throw new Exception("Tried to get a READ lock for non-existing key.");
        }
        return rwLock.readLock();
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    public void clearStorage(){
        File directory = new File(dirStore);
        File[] files = directory.listFiles();
        if (files == null) {
            // no file on disk, clear file in memory
            store.clear();
            fileLocks.clear();
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                // we only want to delete key-value pairs
                continue;
            }
            // get write lock before deleting
            String key = file.getName();
            Lock wLock = getKeyWriteLock(key);
            try {
                wLock.lock();
                logger.debug("Key: <" + key + "> Lock W acquired. (clearStorage)");
                // delete lock, file, memory
                fileLocks.remove(key);
                file.delete();
                store.remove(key);
            } finally {
                // always release the lock
                logger.debug("Key: <" + key + "> Lock W released. (clearStorage)");
                wLock.unlock();
            }
        }
        // in case memory still has files
        store.clear();
        fileLocks.clear();
    }

    /* User-request-related methods */
    /**
     * Get a KV pair from store for the key.
     * @param key an existing key
     * @return value for key, or null if not found
     */
    public String get(String key) throws Exception {
        // must check if the key exists before getting a read lock
        if (!containsKey(key)) {
            return null;
        }
        // get shared read lock before store get
        Lock rLock = getKeyReadLock(key);
        String value = null;
        try {
            rLock.lock();
            logger.debug("Key: <" + key + "> Lock R acquired. (get)");
            value = store.get(key);
        } finally {
            // always release the lock
            logger.debug("Key: <" + key + "> Lock R released. (get)");
            rLock.unlock();
        }
        return value;
    }
    /**
     * Put a new KV pair into store.
     * @param key a key, must not be already existing
     * @param value a value, must not be null
     */
    public void put(String key, String value) throws Exception {
        // get write lock before put
        Lock wLock = getKeyWriteLock(key);
        try {
            wLock.lock();
            logger.debug("Key: <" + key + "> Lock W acquired. (put)");
            store.put(key, value);
            savePairToDisk(key, value);
        } finally {
            // always release the lock
            logger.debug("Key: <" + key + "> Lock W released. (put)");
            wLock.unlock();
        }
    }
    /**
     * Update an existing KV pair in store.
     * @param key an existing key
     * @param value a value, must not be null
     */
    public void update(String key, String value) {
        // get write lock before update
        Lock wLock = getKeyWriteLock(key);
        try {
            wLock.lock();
            logger.debug("Key: <" + key + "> Lock W acquired. (update)");
            store.put(key, value);
            savePairToDisk(key, value);
        } finally {
            // always release the lock
            logger.debug("Key: <" + key + "> Lock W released. (update)");
            wLock.unlock();
        }
    }
    /**
     * Delete an existing KV pair in store.
     * @param key an existing key
     */
    public void delete(String key) {
        // get write lock before update
        Lock wLock = getKeyWriteLock(key);
        try {
            wLock.lock();
            logger.debug("Key: <" + key + "> Lock W acquired. (delete)");
            // delete lock, memory, file
            fileLocks.remove(key);
            store.remove(key);
            File file = new File(dirStore, key);
            file.delete();
        } finally {
            // always release the lock
            logger.debug("Key: <" + key + "> Lock W released. (delete)");
            wLock.unlock();
        }
    }

    private void savePairToDisk(String key, String value) {
        File file = new File(dirStore, key);
        // overwrite existing files
        try (PrintWriter writer = new PrintWriter(new FileWriter(file, false))) {
            writer.print(value);
        } catch (IOException e) {
            String errMsg = "Unable to save key-value pair to disk: " + file.getAbsolutePath();
            System.err.println(errMsg);
            logger.error(errMsg, e);
        }
    }
}
