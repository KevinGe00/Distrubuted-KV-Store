package app_kvClient;

import client.ClientSocketListener;
import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;

public class KVClient implements ClientSocketListener, IKVClient  {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVStoreClient> ";
    private BufferedReader stdin;
    private KVStore client = null;
    private boolean stop = false;
    private LogSetup logSetup = new LogSetup("logs/client.log", Level.ALL);;
    private String serverAddress;
    private int serverPort;

    // most recent mapping of KVServers (defined by their IP:Port) and the associated range of hash value
    private HashMap<String, List<BigInteger>> metadata = new HashMap<>();
    private HashMap<String, List<BigInteger>> metadata_read = new HashMap<>();

    public KVClient() throws IOException {
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    // hash string to MD5 bigint
    private BigInteger hash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes());
            byte[] digest = md.digest();
            return new BigInteger(1, digest);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    private boolean isBounded(BigInteger number, BigInteger range_from, BigInteger range_to) {
        /*
         * Keyrange goes counterclockwise.
         * With wrap-around:        range_to < FF (<) 0 < range_from < range_to
         * Without wrap-around:     range_to < range_from
         */
        boolean bounded;
        if (range_from.compareTo(range_to) < 0) {
            // wrap-around
            BigInteger minHash = new BigInteger("0", 16);
            BigInteger maxHash = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
            if (number.compareTo(range_from) <= 0 && number.compareTo(minHash) >= 0) {
                bounded = true;
            } else if (number.compareTo(maxHash) <= 0 && number.compareTo(range_to) >= 0) {
                bounded = true;
            } else {
                bounded = false;
            }
        } else {
            // normal comparison
            if (number.compareTo(range_from) <= 0 && number.compareTo(range_to) >= 0) {
                bounded = true;
            } else {
                bounded = false;
            }
        }
        return bounded;
    }

    // input is key in KV storage, return the port of its cached coordinator
    private int getCoordinatorPortForKey(String key) {
        BigInteger keyAsHash = hash(key);
        for (String server : metadata.keySet()) {
            List<BigInteger> value = metadata.get(server);
            BigInteger currServerRangeFrom = value.get(0);
            BigInteger currServerRangeTo = value.get(1);

            if (isBounded(keyAsHash, currServerRangeFrom, currServerRangeTo)) {
                int port = Integer.parseInt(server.split(":")[1]);
                logger.debug(">>> For Key '" + key + "', the cached coordinator is #" + port + ". "
                            + "Currently, connected to #" + serverPort + ".");
                return port;
            }
        }
        logger.error("Cannot find the coordinator for Key: '" + key + "'");
        return -1;
    };
    // input is key in KV storage, return a random ports of its cached replicas
    private int getReplicaPortForKey(String key) {
        BigInteger keyAsHash = hash(key);
        List<Integer> portsFound = new ArrayList<>();
        StringBuilder str = new StringBuilder();

        for (String server : metadata_read.keySet()) {
            List<BigInteger> value = metadata_read.get(server);
            BigInteger currServerRangeFrom = value.get(0);
            BigInteger currServerRangeTo = value.get(1);

            if (isBounded(keyAsHash, currServerRangeFrom, currServerRangeTo)) {
                int port = Integer.parseInt(server.split(":")[1]);
                // if currently connecting to a replica, do not switch
                if (port == serverPort) {
                    logger.debug(">>> For Key '" + key + "', one of cached replicas is #"
                                + port + ". Currently, connected to #" + serverPort + ".");
                    return port;
                }
                portsFound.add(port);
                str.append(" #" + port);
            }
        }
        logger.debug(">>> For Key '" + key + "', the cached replicas are"
                    + str.toString() + ". Currently, connected to #" + serverPort + ".");
        Collections.shuffle(portsFound);
        int port = -1;
        if (portsFound.size() > 0) {
            port = portsFound.get(0);
        } else {
            logger.error("Cannot find any replica for Key: '" + key + "'");
        }
        return port;
    };

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                    /* manual connect also update keyrange */
                    KVMessage msg = client.getKeyrange();
                    switch (msg.getStatus()) {
                        case KEYRANGE_SUCCESS:
                            String valueKVMsg = msg.getValue();
                            updateMetadata(valueKVMsg);
                            break;
                        case SERVER_STOPPED:
                            printError("Connected, but did not received metadata "
                                        + "because the server is stopped.");
                            break;
                        default:
                            printError("Unexpected server response: <"
                                        + msg.getStatus().name()
                                        + ">");
                            logger.error("The KVMessage from server is invalid.");
                    }
                    /* manual connect also update keyrange_read */
                    msg = client.getKeyrangeRead();
                    switch (msg.getStatus()) {
                        case KEYRANGE_READ_SUCCESS:
                            String valueKVMsg = msg.getValue();
                            updateMetadataRead(valueKVMsg);
                            break;
                        case SERVER_STOPPED:
                            printError("Did not receive metadata_read, ether.");
                            break;
                        default: 
                            printError("Unexpected server response: <"
                                    + msg.getStatus().name()
                                    + ">");
                            logger.error("The KVMessage from server is invalid.");
                    }
                } catch(NumberFormatException nfe) {
                    printError("No valid port. Port must be a number!");
                    logger.info("Unable to parse argument 'port'. ", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host! ", e);
                } catch (Exception e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection! ", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("disconnect")) {
            disconnect();
        } else if(tokens[0].equals("get")) {
            /* GET request */
            if(tokens.length == 2) {
                try{
                    String key = tokens[1];
                    // hidden from user: re-connect to the right server, if necessary
                    switchToAnyReplica(key);
                    // GET
                    KVMessage msg = client.get(key);
                    // hidden from user: if metadata_read is outdated, update and retry
                    while (needToGetAgain(msg)) {
                        logger.debug(">>> Updated metadata_read cache, retrying GET.");
                        switchToAnyReplica(key);
                        msg = client.get(key);
                    }

                    switch (msg.getStatus()) {
                        case GET_SUCCESS:
                            System.out.println(PROMPT + "Got value: "
                                               + msg.getValue());
                            break;
                        case GET_ERROR:
                            printError("Server cannot get value for key: <"
                                       + msg.getKey()
                                       + ">");
                            break;
                        case SERVER_STOPPED:
                            printError("Server is not running, cannot get key: <"
                                       + msg.getKey()
                                       + ">");
                            break;
                        default:
                            printError("Unexpected server response: <"
                                       + msg.getStatus().name()
                                       + ">");
                            logger.error("The KVMessage from server is not for GET.");
                    }
                } catch (Exception e) {
                    printError("GET command failure.");
                    logger.error("Client's GET operation failed.", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("put")) {
            /* PUT request */
            if ((tokens.length < 2) || (tokens.length > 3)) {
                printError("Invalid number of parameters!");
                return;
            }
            String key = tokens[1];
            String value = "";
            if (tokens.length == 2) {
                /* DELETE */
                value = null;
            } else {
                /* PUT or UPDATE */
                value = tokens[2];
            }
            try {
                // hidden from user: re-connect to the right server, if necessary
                switchToTheCoordinator(key);
                // PUT
                KVMessage msg = client.put(key, value);
                // hidden from user: if metadata is outdated, update and retry
                while (needToPutAgain(msg)) {
                    logger.debug(">>> Updated metadata cache, retrying PUT.");
                    switchToTheCoordinator(key);
                    msg = client.put(key, value);
                }

                switch (msg.getStatus()) {
                    case PUT_SUCCESS:
                        System.out.println(PROMPT + "PUT succeeded.");
                        break;
                    case PUT_UPDATE:
                        System.out.println(PROMPT + "UPDATE succeeded.");
                        break;
                    case PUT_ERROR:
                        printError("Server cannot put key-value pair: <"
                                   + msg.getKey() + ">-<"
                                   + msg.getValue()
                                   + ">");      
                        break;
                    case DELETE_SUCCESS:
                        System.out.println(PROMPT + "DELETE succeeded.");
                        break;
                    case DELETE_ERROR:
                        printError("Server cannot delete key-value pair "
                                   + "for key: <"
                                   + msg.getKey()
                                   + ">");
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, cannot put key-value"
                                    + " pair: <" + msg.getKey() + ">-<"
                                    + msg.getValue()
                                    + ">");
                        break;
                    case SERVER_WRITE_LOCK:
                        printError("Server is locked for WRITE, cannot put key"
                                    + "-value pair: <" + msg.getKey() + ">-<"
                                    + msg.getValue()
                                    + ">");
                        break;
                    default:
                        printError("Unexpected server response: <"
                                   + msg.getStatus().name()
                                   + ">");
                        logger.error("The KVMessage from server is not for PUT.");
                }
            } catch (Exception e) {
                printError("PUT command failure.");
                logger.error("Client's PUT operation failed.", e);
            }
        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = logSetup.setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else if(tokens[0].equals("keyrange")) {
            /* KEYRANGE command */
            try {
                KVMessage msg = client.getKeyrange();
                switch (msg.getStatus()) {
                    case KEYRANGE_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        System.out.println(PROMPT + "Keyrange: " + valueKVMsg);
                        updateMetadata(valueKVMsg);
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, ignored 'Keyrange'");
                        break;
                    default:
                        printError("Unexpected server response: <"
                                   + msg.getStatus().name()
                                   + ">");
                        logger.error("The KVMessage from server is not for"
                                    + " 'Keyrange'.");
                }
            } catch (Exception e) {
                printError("'Keyrange' command failure.");
                logger.error("Client's 'Keyrange' command failed.", e);
            }
        } else if(tokens[0].equals("keyrange_read")) {
            /* KEYRANGE_READ command */
            try {
                KVMessage msg = client.getKeyrangeRead();
                switch (msg.getStatus()) {
                    case KEYRANGE_READ_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        System.out.println(PROMPT + "Keyrange Read: " + valueKVMsg);
                        updateMetadataRead(valueKVMsg);
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, ignored 'Keyrange_Read'");
                        break;
                    default: 
                        printError("Unexpected server response: <"
                                   + msg.getStatus().name()
                                   + ">");
                        logger.error("The KVMessage from server is not for"
                                    + " 'Keyrange_Read'.");
                }
            } catch (Exception e) {
                printError("'Keyrange_Read' command failure.");
                logger.error("Client's 'Keyrange_Read' command failed.", e);
            }
        } else if (tokens[0].equals("table_put")) {
            /* M4 */
            if (tokens.length != 5) {
                printError("Invalid number of parameters! Expect 'table_put key row col value'");
                return;
            }
            String key = tokens[1];
            String row = tokens[2];
            String col = tokens[3];
            String value = tokens[4];
            try {
                // M3 copy
                switchToTheCoordinator(key);
                // TABLE_PUT
                KVMessage msg = client.table_put(key, row, col, value);
                // M3 copy, TABLE_PUT share this with normal PUT
                while (needToPutAgain(msg)) {
                    logger.debug(">>> Updated metadata cache, retrying TABLE_PUT.");
                    switchToTheCoordinator(key);
                    msg = client.table_put(key, row, col, value);
                }

                // M3 copy with slight modification
                switch (msg.getStatus()) {
                    case TABLE_PUT_SUCCESS:
                        System.out.println(PROMPT + "TABLE_PUT succeeded.");
                        break;
                    case TABLE_PUT_FAILURE:
                        printError("Server cannot table_put: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, cannot table_put: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    case SERVER_WRITE_LOCK:
                        printError("Server is locked for WRITE, cannot table_put: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    default:
                        printError("Unexpected server response: <"
                                    + msg.getStatus().name()
                                    + ">");
                        logger.error("The KVMessage from server is not for TABLE_PUT.");
                }
            } catch (Exception e) {
                printError("TABLE_PUT command failure.");
                logger.error("Client's TABLE_PUT operation failed.", e);
            }
        } else if (tokens[0].equals("table_delete")) {
            /* M4 */
            if (tokens.length != 4) {
                printError("Invalid number of parameters! Expect 'table_delete key row col'");
                return;
            }
            String key = tokens[1];
            String row = tokens[2];
            String col = tokens[3];
            try {
                // M3 copy
                switchToTheCoordinator(key);
                // TABLE_DELETE
                KVMessage msg = client.table_delete(key, row, col);
                // M3 copy, TABLE_DELETE share this with normal PUT
                while (needToPutAgain(msg)) {
                    logger.debug(">>> Updated metadata cache, retrying TABLE_DELETE.");
                    switchToTheCoordinator(key);
                    msg = client.table_delete(key, row, col);
                }

                // M3 copy with slight modification
                switch (msg.getStatus()) {
                    case TABLE_DELETE_SUCCESS:
                        System.out.println(PROMPT + "TABLE_DELETE succeeded.");
                        printTableCell(msg.getValue());
                        break;
                    case TABLE_DELETE_FAILURE:
                        printError("Server cannot table_delete: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, cannot table_delete: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    case SERVER_WRITE_LOCK:
                        printError("Server is locked for WRITE, cannot table_delete: <"
                                    + msg.getKey() + ">-<"
                                    + msg.getValue() + ">");
                        break;
                    default:
                        printError("Unexpected server response: <"
                                    + msg.getStatus().name()
                                    + ">");
                        logger.error("The KVMessage from server is not for TABLE_DELETE.");
                }
            } catch (Exception e) {
                printError("TABLE_DELETE command failure.");
                logger.error("Client's TABLE_DELETE operation failed.", e);
            }
        } else if (tokens[0].equals("table_get")) {
            /* M4 */
            if (tokens.length != 4) {
                printError("Invalid number of parameters! Expect 'table_get key row col'");
                return;
            }
            String key = tokens[1];
            String row = tokens[2];
            String col = tokens[3];
            try{
                // M3 copy
                switchToAnyReplica(key);
                // TABLE_GET
                KVMessage msg = client.table_get(key, row, col);
                // M3 copy, TABLE_GET share this with normal GET
                while (needToGetAgain(msg)) {
                    logger.debug(">>> Updated metadata_read cache, retrying TABLE_GET.");
                    switchToAnyReplica(key);
                    msg = client.table_get(key, row, col);
                }

                // M3 copy with slight modification
                switch (msg.getStatus()) {
                    case TABLE_GET_SUCCESS:
                        System.out.println(PROMPT + "Got table value:");
                        printTableCell(msg.getValue());
                        break;
                    case TABLE_GET_FAILURE:
                        printError("Server cannot table_get for key: <"
                                    + msg.getKey() + "> with ("
                                    + row + ", " + col + ")");
                        break;
                    case SERVER_STOPPED:
                        printError("Server is not running, cannot table_get for key: <"
                        + msg.getKey() + "> with ("
                        + row + ", " + col + ")");
                        break;
                    default:
                        printError("Unexpected server response: <"
                                    + msg.getStatus().name()
                                    + ">");
                        logger.error("The KVMessage from server is not for TABLE_GET.");
                }
            } catch (Exception e) {
                printError("TABLE_GET command failure.");
                logger.error("Client's TABLE_GET operation failed.", e);
            }
        } else if (tokens[0].equals("table_select")) {
            /* M4 */
            if ((tokens.length != 4) || (!tokens[2].equals("from"))) {
                printError("Invalid number of parameters! Expect 'table_select col_A,...,col_X>50,col_Y<50 from key_1,...,key_n'");
                return;
            }
            // M4 new
            String keys_in_one = tokens[3];
            String cols_cond_linebreak = tokens[1].replace(",", System.lineSeparator());
            try {
                String[] keys_array = keys_in_one.split(",");
                boolean allSuccess = true;
                ArrayList<String> tables_string = new ArrayList<>();

                // for each key, contact its corresponding server
                loop: for (String key : keys_array) {
                    // M3 copy
                    switchToAnyReplica(key);
                    // SELECT
                    KVMessage msg = client.table_select(key, cols_cond_linebreak);
                    // M3 copy, TABLE_SELECT share this with normal GET
                    while (needToGetAgain(msg)) {
                        logger.debug(">>> Updated metadata_read cache, retrying TABLE_SELECT.");
                        switchToAnyReplica(key);
                        msg = client.table_select(key, cols_cond_linebreak);
                    }

                    // M4 new switch
                    switch (msg.getStatus()) {
                        case TABLE_SELECT_SUCCESS:
                            allSuccess = allSuccess && true;
                            tables_string.add(msg.getValue());
                            break;
                        case TABLE_SELECT_FAILURE:
                            allSuccess = false;
                            printError("Server cannot table_select: <"
                                       + msg.getKey() + ">-<" + msg.getValue() + ">");
                            break loop;
                        case SERVER_STOPPED:
                            allSuccess = false;
                            printError("Server is not running, cannot table_select: <"
                                       + msg.getKey() + ">-<" + msg.getValue() + ">");
                            break loop;
                        default:
                            allSuccess = false;
                            printError("Unexpected server response: <"
                                        + msg.getStatus().name()
                                        + ">");
                            logger.error("The KVMessage from server is not for TABLE_SELECT.");
                            break loop;
                    }
                }

                // if all success, print table;
                // if any failure, error.
                if (allSuccess) {
                    System.out.println(PROMPT + "TABLE_SELECT succeeded.");
                    printAllTables(tables_string);
                } // error has already been printed, no else
            } catch (Exception e) {
                printError("SELECT command failure.");
                logger.error("Client's SELECT operation failed.", e);
            }
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    /* M4 */
    private void printAllTables(ArrayList<String> tables_string) {
        StringBuilder text = new StringBuilder();
        int idx_first = 0;
        for (int idx = 0; idx < tables_string.size(); idx++) {
            if (tables_string == null) {
                idx_first = idx_first + 1;
                continue;
            }
            String table_string = tables_string.get(idx);
            String subText = getPrintSingleTable(table_string);
            if (idx != idx_first) {
                // remove column name of later table
                subText = subText.substring(subText.indexOf(System.lineSeparator())
                                            + System.lineSeparator().length());
            }
            text.append(subText + System.lineSeparator());
        }
        System.out.println(text.toString());
    }

    /* M4 */
    private String getPrintSingleTable(String table_string) {
        String doubleLineSep = System.lineSeparator() + System.lineSeparator();
        String singleLineSep = System.lineSeparator();

        String[] rows_cols_values = table_string.split(doubleLineSep, 3);
        String[] rows = rows_cols_values[0].split(singleLineSep);
        String[] cols = rows_cols_values[1].split(singleLineSep);
        String[] values = rows_cols_values[2].split(singleLineSep);

        int num_row = rows.length;
        int num_col = cols.length;

        StringBuilder text = new StringBuilder();
        
        // column name line
        text.append(" ");
        for (int idx = 0; idx < num_col; idx++) {
            text.append(" \t" + cols[idx]);
        }

        // rows
        for (int idx_row = 0; idx_row < num_row; idx_row++) {
            text.append(singleLineSep);
            text.append(rows[idx_row]);
            // values (in each column)
            for (int idx_col = 0; idx_col < num_col; idx_col++) {
                int idx_value = idx_row * num_col + idx_col;
                text.append(" \t" + values[idx_value]);
            }
        }

        return text.toString();
    }

    /* M4 */
    private void printTableCell(String content) {
        /* content string structure: "row
         *                            col
         *                            value"
         */
        String[] row_col_value = content.split(System.lineSeparator());
        System.out.println(" \t" + row_col_value[1] + System.lineSeparator()
                           + row_col_value[0] + " \t" + row_col_value[2]);
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        disconnect();
        client = new KVStore(hostname, port);
        client.connect();
        client.addListener(this);
        client.start();
    }

    private void disconnect() {
        if(client != null) {
            client.disconnect();
            client = null;
        }
    }

    /* Keyrange-related methods */
    /**
     * Check if current connected server is the coordinator for this key,
     * based on cached metadata. If not, attempt re-connection.
     * @param key the key in the request KVMessage soon to be sent
     * @return true for did reconnect, false otherwise
     */
    private boolean switchToTheCoordinator(String key) throws Exception {
        int portCopy = serverPort;
        int portCoordinator = getCoordinatorPortForKey(key);
        if (serverPort != portCoordinator) {
            serverPort = portCoordinator;
            try{
                newConnection(serverAddress, serverPort);
                return true;
            } catch (Exception e) {
                // if cannot connect to the suggested port, reconnect to its original port
                logger.debug(">>> Cannot connect to the cached coordinator #" + portCoordinator
                            + ", resuming prior connection with #" + portCopy + ".");
                serverPort = portCopy;
                newConnection(serverAddress, serverPort);
                KVMessage msg = client.getKeyrange();
                switch (msg.getStatus()) {
                    case KEYRANGE_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        updateMetadata(valueKVMsg);
                        break;
                    default:
                        logger.debug(">>> Cannot update metadata right now, need to retry later.");
                }
                msg = client.getKeyrangeRead();
                switch (msg.getStatus()) {
                    case KEYRANGE_READ_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        updateMetadataRead(valueKVMsg);
                        break;
                    default: 
                        logger.debug(">>> Cannot update metadata_read right now, need to retry later.");
                }
                return false;
            }
        }
        return false;
    }
    /**
     * Check if current connected server is one of the replicas for this key,
     * based on cached metadata_read. If not, attempt re-connection.
     * @param key the key in the request KVMessage soon to be sent
     * @return true for did reconnect, false otherwise
     */
    private boolean switchToAnyReplica(String key) throws Exception {
        int portCopy = serverPort;
        int portReplica = getReplicaPortForKey(key);
        if (serverPort != portReplica) {
            serverPort = portReplica;
            try{
                newConnection(serverAddress, serverPort);
                return true;
            } catch (Exception e) {
                // if cannot connect to the suggested port, reconnect to its original port
                logger.debug(">>> Cannot connect to the cached replica #" + portReplica
                            + ", resuming prior connection with #" + portCopy + ".");
                serverPort = portCopy;
                newConnection(serverAddress, serverPort);
                KVMessage msg = client.getKeyrange();
                switch (msg.getStatus()) {
                    case KEYRANGE_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        updateMetadata(valueKVMsg);
                        break;
                    default:
                        logger.debug(">>> Cannot update metadata right now, need to retry later.");
                }
                msg = client.getKeyrangeRead();
                switch (msg.getStatus()) {
                    case KEYRANGE_READ_SUCCESS:
                        String valueKVMsg = msg.getValue();
                        updateMetadataRead(valueKVMsg);
                        break;
                    default: 
                        logger.debug(">>> Cannot update metadata_read right now, need to retry later.");
                }
                return false;
            }
        }
        return false;
    }
    /**
     * Check if server's response is NOT_RESPONSIBLE, and re-connect if needed
     * @param kvMsg KVMessage received from recent PUT request
     * @return true for needing to request again, false otherwise
     */
    private boolean needToPutAgain(KVMessage kvMsg) throws Exception {
        if (kvMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
            return false;
        }
        String valueKVMsg = kvMsg.getValue();
        updateMetadata(valueKVMsg);
        return true;
    }
    /**
     * Check if server's response is NOT_RESPONSIBLE, and re-connect if needed
     * @param kvMsg KVMessage received from recent GET request
     * @return true for needing to request again, false otherwise
     */
    private boolean needToGetAgain(KVMessage kvMsg) throws Exception {
        if (kvMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
            return false;
        }
        String valueKVMsg = kvMsg.getValue();
        updateMetadataRead(valueKVMsg);
        return true;
    }
    /**
     * Use value from KVMessage KEYRANGE_SUCCESS to update client's cache metadata
     * @param valueKVMsg getValue() from KEYRANGE_SUCCESS KVMessage
     */
    private void updateMetadata(String valueKVMsg) throws Exception {
        if (valueKVMsg == null) {
            return;
        }
        metadata = convertStringToMetaHashmap(valueKVMsg);
    }
    /**
     * Use value from KVMessage KEYRANGE_SUCCESS_READ to update client's cache metadata_read
     * @param valueKVMsg getValue() from KEYRANGE_SUCCESS_READ KVMessage
     */
    private void updateMetadataRead(String valueKVMsg) throws Exception {
        if (valueKVMsg == null) {
            return;
        }
        metadata_read = convertStringToMetaHashmap(valueKVMsg);
    }
    // convert string: "range_from,range_to,ip,port;...;range_from,range_to,ip,port" to metadata hashmap
    private HashMap<String, List<BigInteger>> convertStringToMetaHashmap(String str) {
        HashMap<String, List<BigInteger>> metaHashmap = new HashMap<>();
        String[] subStrs = str.split(";");
        for (String subStr : subStrs) {
            String[] rFrom_rTo_ip_port = subStr.split(",");
            String ip = rFrom_rTo_ip_port[2];
            String port = rFrom_rTo_ip_port[3];
            BigInteger range_from = new BigInteger(rFrom_rTo_ip_port[0], 16);
            BigInteger range_to = new BigInteger(rFrom_rTo_ip_port[1], 16);
            String fullAddess = ip + ":" + port;
            List<BigInteger> bigIntFrom_bigIntTo = Arrays.asList(range_from, range_to);
            metaHashmap.put(fullAddess, bigIntFrom_bigIntTo);
        }
        return metaHashmap;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV-STORE CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value> \n");
        sb.append("\t Inserts a key-value pair into the storage server data structures.\n");
        sb.append("\t Updates (overwrites) the current value with the given value if the server already contains the specified key.\n");
        sb.append("\t Deletes the entry for the given key if <value> equals null.\n");

        sb.append(PROMPT).append("get <key> \n");
        sb.append("\t Retrieves the value for the given key from the storage server, if it exists.\n");

        sb.append(PROMPT).append("keyrange \n");
        sb.append("\t Retrieves latest server keyrange metadata from a running storage server.\n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }


    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public KVCommInterface getStore() {
        return client;
    }
}
