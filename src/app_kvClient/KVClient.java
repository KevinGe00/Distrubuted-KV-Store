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
            BigInteger minHash = new BigInteger("0");
            BigInteger maxHash = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
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

    // input is key in KV storage, return the port of the (optimistically) KVServer responsible for it
    private int getServerPortResponsibleForKey(String key) {
        BigInteger keyAsHash = hash(key);

        for (String server : metadata.keySet()) {
            List<BigInteger> value = metadata.get(server);
            BigInteger currServerRangeStart = value.get(0);
            BigInteger currServerRangeEnd = value.get(1);

            if (currServerRangeEnd == BigInteger.ZERO) {
                if (keyAsHash.compareTo(currServerRangeStart) >= 0) {
                    // file is hashed to the end of the hash ring between last server (highest hash value) and dummy between
                    int port = Integer.parseInt(server.split(":")[1]); // keys in metadata are hostname:port
                    return port;
                }
            } else {
                if (isBounded(keyAsHash, currServerRangeStart, currServerRangeEnd)) {
                    if (currServerRangeStart == BigInteger.ZERO) {
                        // Handling dummy node case:
                        // if our key lands in the dummy node's key range, it's actually the dummy node's predecessor
                        // that should handle this key, we need to find that predecessor
                        return getLastNodeInHashRing();
                    } else {
                        // otherwise, simply return this server's port
                        int port = Integer.parseInt(server.split(":")[1]); // keys in metadata are hostname:port
                        return port;
                    }
                }
            }
        }
        logger.error("could find server port for key: " + key);
        return 0;
    };

    // get port of logically highest node in ring
    private int getLastNodeInHashRing() {
        for (String s : metadata.keySet()) {
            List<BigInteger> val = metadata.get(s);
            BigInteger serverRangeEnd = val.get(1);
            if (serverRangeEnd == BigInteger.ZERO) {
                int port = Integer.parseInt(s.split(":")[1]); // keys in metadata are hostname:port
                return port;
            }
        }
        return 0;
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
                } catch(NumberFormatException nfe) {
                    printError("No valid port. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (Exception e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
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
                    if (hasReconnected(key)) {
                        logger.debug("For GET <" + key + ">, reconnected to port "
                                    + serverPort + ".");
                    }
                    // GET
                    KVMessage msg = client.get(key);
                    // hidden from user: if metadata is outdated, update and retry
                    while (needToRequestAgain(msg)) {
                        logger.debug("Keyrange metadata outdated, and has been updated."
                                    + " Retrying GET <" + key + ">.");
                        if (hasReconnected(key)) {
                            logger.debug("For GET <" + key + ">, reconnected to port "
                                        + serverPort + ".");
                        }
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
                if (hasReconnected(key)) {
                    logger.debug("For PUT <" + key + ">, reconnected to port "
                                + serverPort + ".");
                }
                // PUT
                KVMessage msg = client.put(key, value);
                // hidden from user: if metadata is outdated, update and retry
                while (needToRequestAgain(msg)) {
                    logger.debug("Keyrange metadata outdated, and has been updated."
                                + " Retrying PUT <" + key + ">.");
                    if (hasReconnected(key)) {
                        logger.debug("For PUT <" + key + ">, reconnected to port "
                                    + serverPort + ".");
                    }
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
                                    + " pair: <" + msg.getValue() + ">-<"
                                    + msg.getValue()
                                    + ">");
                        break;
                    case SERVER_WRITE_LOCK:
                        printError("Server is locked for WRITE, cannot put key"
                                    + "-value pair: <" + msg.getValue() + ">-<"
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
                        updateKeyrangeMetadata(valueKVMsg);
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
        } else {
            printError("Unknown command");
            printHelp();
        }
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
     * Check if current connected server is responsible for this key,
     * based on cached keyrange metadata. If so, attempt re-connection.
     * @param key the key in the request KVMessage soon to be sent
     * @return true for did reconnect, false otherwise
     */
    private boolean hasReconnected(String key) throws Exception {
        int portCopy = serverPort;
        int portResponsible = getServerPortResponsibleForKey(key);
        if (serverPort != portResponsible) {
            serverPort = portResponsible;
            try{
                newConnection(serverAddress, serverPort);
                return true;
            } catch (Exception e) {
                // if cannot connect to the suggested port, reconnect to its original port
                logger.warn("Tried to reconnect to suggested port " + portResponsible
                            + ", but was unable to connect. Reconnecting to original port "
                            + portCopy + ".", e);
                serverPort = portCopy;
                newConnection(serverAddress, serverPort);
                return false;
            }
        }
        return false;
    }
    /**
     * Check if server's response is NOT_RESPONSIBLE, and re-connect if needed
     * @param kvMsg KVMessage received from recent PUT or GET request
     * @return true for needing to request again, false otherwise
     */
    private boolean needToRequestAgain(KVMessage kvMsg) throws Exception {
        if (kvMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
            return false;
        }
        String valueKVMsg = kvMsg.getValue();
        updateKeyrangeMetadata(valueKVMsg);
        return true;
    }
    /**
     * Use value from KVMessage KEYRANGE_SUCCESS to update client's cache metadata
     * @param valueKVMsg getValue() from KEYRANGE_SUCCESS KVMessage
     */
    private void updateKeyrangeMetadata(String valueKVMsg) throws Exception {
        if (valueKVMsg == null) {
            return;
        }
        metadata = convertStringToMetaHashmap(valueKVMsg);
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
