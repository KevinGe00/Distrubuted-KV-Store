package app_kvServer;

public interface IKVServer {
    /* Server status enum */
    public enum SerStatus {
        STOPPED,        // server stopped, no incoming request accepts
        RUNNING,        // server has started and been running
        WRITE_LOCK,     // server denies PUT request
        SHUTTING_DOWN   // server shuts down, waiting for children
    }
    /**
     * Set status of the server; cannot change from SHUTTING_DOWN
     * Synchronized with a lock.
     * @param status a IKVServer.SerStatus
     * @return true for success; false otherwise
     */
    public boolean setSerStatus(SerStatus status);
    /**
     * Get status of the server.
     * @return a IKVServer.SerStatus
     */
    public SerStatus getSerStatus();

    /* Port */
    /**
     * Get port of this server. Cannot re-set port/
     * @return port
     */
    public int getPort();

    /* Hostname */
    /**
     * One-time set hostname of the server.
     * @param hostname hostname of server
     * @return true for success; false otherwise
     */
    public boolean setHostname(String hostname);
    /**
     * Get hostname of this server.
     * @return hostname of server
     */
    public String getHostname();

    /* Store */
    /**
     * One-time initialize disk storage of the server
     * @param storeDirectory directory of disk storage
     * @return true for success, false for failure
     */
    public boolean initializeStore(String storeDirectory);
    /**
     * Check if the key is in server storage
     * @return true if so; false otherwise
     */
    public boolean inStorage(String key);
    /**
     * Clear the disk storage of the server.
     * @return true for success; false otherwise
     */
    public boolean clearStorage();

    /* Test method: get & put */
    /**
     * Get value for the key from the server (for test-only)
     * @param key the key
     * @return value for the key
     * @throws Exception when key cannot be found, or other exception
     */
    public String getKV(String key) throws Exception;
    /**
     * Put key-value pair into the server (for test-only)
     * @param key the key
     * @param value the value
     * @throws Exception when operation failed
     */
    public void putKV(String key, String value) throws Exception;

    /* Server control */
    /**
     * Run the server on current thread. Do not return until termination.
     * To avoid blocking, run this command in its own thread.
     */
    public void run();
    /**
     * Kill the server without the full shutdown process.
     * This should not be called unless it is absolutely necessary.
     */
    public void kill();
    /**
     * Shutdown the server properly.
     */
    public void close();
}
