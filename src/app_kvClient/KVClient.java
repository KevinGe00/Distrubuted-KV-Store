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
import java.net.UnknownHostException;

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

    // cached server metadata
    private String metadata = null;

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

    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    disconnect();
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
                    KVMessage msg = client.get(key);
                    // check if it is keyrange


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
                KVMessage msg = client.put(key, value);
                // check if it is keyrange



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
                        System.out.println(PROMPT + "Keyrange: " + msg.getValue());
                        metadata = msg.getValue();
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

    /**
     * Check if server's response is NOT_RESPONSIBLE, and re-connect if needed
     * @param kvMsg KVMessage received from recent PUT or GET request
     * @return true for needing to request again, false for 
     */
    private boolean needToRequestAgain(KVMessage kvMsg){
        if (kvMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
            return false;
        }
        // to-do


        
        return true;
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
