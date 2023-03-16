package app_kvECS;

import app_kvServer.KVServerChild;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import shared.messages.KVMessage;

import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ECSClient implements IECSClient {
    /**
     * Datatype containing a pair of response socket and thread.
     */
    class ChildObject {
        public Thread childThread;
        public Socket childSocket;
    }

    class RemovedNode {
        public Boolean success;
        public int port;
        public String storeDir;
        public List<BigInteger> range;
        public String successor;
    }

    public static class Mailbox {
        public boolean needsToSendWriteLock;
        public String valueSend_forWriteLock;
    }

    private static Logger logger = Logger.getRootLogger();
    private String address;
    private int port;
    private boolean running = true;
    private ServerSocket serverSocket;
    private BufferedReader stdin;
    public HashMap<String, ChildObject> childObjects;
    public HashMap<String, String> successors;
    public HashMap<String, String> predecessors;
    public HashMap<String, Mailbox> childMailboxs;

    public HashMap<BigInteger, IECSNode> getHashRing() {
        return hashRing;
    }

    private HashMap<BigInteger, IECSNode> hashRing = new HashMap<>();
    //mapping of KVServers (defined by their IP:Port) and the associated range of hash value
    private HashMap<String, List<BigInteger>> metadata = new HashMap<>();
    private ArrayList<Thread> clients = new ArrayList<Thread>();
    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;
    }
    public HashMap<String, List<BigInteger>> getMetadata() {
        return metadata;
    }

    public boolean getRunning(){
        return running;
    }
    /* Port */
    public int getPort(){
        return port;
    }
    public void initializeServer() {
        logger.info("Initialize ECS server socket ...");
        // find IP for host
        InetAddress bindAddr;
        try {
            bindAddr = InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            logger.error("Error! IP address for host '" + address
                    + "' cannot be found.");
            return;
        }
        // create socket with IP and port
        try {
            serverSocket = new ServerSocket(port, 0, bindAddr);
            serverSocket.setReuseAddress(true);
            logger.info("ECS server host: " + serverSocket.getInetAddress().getHostName()
                    + " \tport: " + serverSocket.getLocalPort());
        } catch (IOException e) {
            logger.error("Error! Cannot open ECS server socket on host: '"
                    + address + "' \tport: " + port);
            closeServerSocket();
            return;
        }

        // initialize client object(thread + socket) arraylist
        childObjects = new HashMap<>();

        successors = new HashMap<>();
        predecessors = new HashMap<>();

        childMailboxs = new HashMap<>();
    }

    /*
     * close the listening socket of the server.
     * should not be directly called in run().
     */
    private void closeServerSocket() {
        if (serverSocket != null) {
            try {
                serverSocket.close();
                serverSocket = null;
            } catch (Exception e) {
                logger.error("Unexpected Error! " +
                        "Unable to close socket on host: '" + address
                        + "' \tport: " + port, e);
            }
        }
    }
    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    /**
     * @param serverHost host name of server to be added as node
     * @param serverPort port of server to be added as node
     * @param storeDir path to storage directory of server
     * @return true on success, otherwise false
     */
    public boolean addNewNode(String serverHost, int serverPort, String storeDir) {
        logger.info("Attempting to add new server " + serverHost + ":" + serverPort + " to ecs.");
        try {
            String fullAddress =  serverHost + ":" + serverPort;
            BigInteger position = addNewServerNodeToHashRing(serverHost, serverPort, storeDir);
            updateMetadataWithNewNode(fullAddress, position);
            putEmptyMailbox(fullAddress);
            logger.info("Successfully added new server " + serverHost + ":" + serverPort + " to ecs.");
            return true;
        } catch (Exception e) {
            logger.error("Unsuccessfully added new server " + serverHost + ":" + serverPort + " to ecs.");
            return false;
        }
    }

    /**
     * @param serverHost host name of server removed
     * @param serverPort port of server removed
     * @return true and storage dir of removed node, and it's keyrange on success, otherwise false and null string otherwise
     */
    public RemovedNode removeNode(String serverHost, int serverPort) {
        RemovedNode rn =  new RemovedNode();
        logger.info("Attempting to remove server " + serverHost + ":" + serverPort + " from ecs.");
        try {
            String fullAddress =  serverHost + ":" + serverPort;
            String removedNodeStoreDir = removeServerNodeFromHashRing(serverHost, serverPort);
            List<BigInteger> removedNodeRange = updateMetadataAfterNodeRemoval(fullAddress);

            rn.success = true;
            rn.storeDir = removedNodeStoreDir;
            rn.range = removedNodeRange;
            rn.successor = successors.get(fullAddress);

            successors.remove(fullAddress);
            childMailboxs.remove(fullAddress);  // remove this node's mailbox

            logger.info("Successfully removed server " + serverHost + ":" + serverPort + " from ecs.");
            return rn;
        } catch (Exception e) {
            rn.success = false;
            logger.error("Unsuccessfully removed server " + serverHost + ":" + serverPort + " from ecs.");
            return rn;
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

    /**
     * @param fullAddress IP:port of newly added server
     * @param position the hash of fullAddress, used to determine the position of this node on the hashring
     */
    public void updateMetadataWithNewNode(String fullAddress, BigInteger position) {
        BigInteger maxvalue = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
        List<BigInteger> newNodeRange;

        for (String key : metadata.keySet()) {
            List<BigInteger> keyrange = metadata.get(key);
            BigInteger range_start = keyrange.get(0);
            BigInteger range_to = keyrange.get(1);

            List<BigInteger> newSuccessorRange;
            if (isBounded(position, range_start, range_to)) {
                if (position.equals(maxvalue)) {
                    newSuccessorRange = Arrays.asList(range_start, BigInteger.ZERO);
                } else {
                    newSuccessorRange = Arrays.asList(range_start, position.add(BigInteger.ONE));
                }

                metadata.put(key, newSuccessorRange);
                logger.info("Cut the range of the successor node " + key + " to " + newSuccessorRange);

                newNodeRange = Arrays.asList(position, range_to);
                metadata.put(fullAddress, newNodeRange);
                logger.info("Figured out hash range for " + fullAddress + " as " + newNodeRange);

                successors.put(fullAddress, successors.get(key)); // new node's successors old successor
                successors.put(key, fullAddress);

                predecessors.put(fullAddress, key);
                predecessors.put(successors.get(fullAddress), fullAddress);

                logger.info("Successor of " + fullAddress + " is " + key);
                
                return;
            }
        }

        // First node added
        if (position.equals(maxvalue)) {
            newNodeRange = Arrays.asList(maxvalue, BigInteger.ZERO);
        } else {
            newNodeRange = Arrays.asList(position, position.add(BigInteger.ONE));
        }

        metadata.put(fullAddress, newNodeRange);
        logger.info("Figured out hash range for " + fullAddress + " as " + newNodeRange);

        // first node is it's own successor
        successors.put(fullAddress, fullAddress);
        predecessors.put(fullAddress, fullAddress);
    }


    // hash string to MD5 bigint
    private BigInteger hash(String fullAddress) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(fullAddress.getBytes());
            byte[] digest = md.digest();
            return new BigInteger(1, digest);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }
    private BigInteger addNewServerNodeToHashRing(String serverHost, int serverPort, String storeDir) {
        String fullAddress =  serverHost + ":" + serverPort;
        BigInteger hashInt = hash(fullAddress);

        // Create new ECSNode
        String nodeName = "Server:" + fullAddress;
        ECSNode node = new ECSNode(nodeName, serverHost, serverPort, storeDir);

        hashRing.put(hashInt, node);
        return hashInt;
    }

    private String removeServerNodeFromHashRing(String serverHost, int serverPort) {
        String fullAddress =  serverHost + ":" + serverPort;
        BigInteger key = hash(fullAddress);

        if (!this.hashRing.containsKey(key)) {
            System.out.println("Error: Key '" + key + "' not found in hashmap");
            return "";
        } else {
            String removedNodeStoreDir = this.hashRing.get(key).getStoreDir();

            this.hashRing.remove(key);
            return removedNodeStoreDir;
        }
    };

    /**
     * @param fullAddress IP:port of removed server
     */
    public List<BigInteger> updateMetadataAfterNodeRemoval (String fullAddress) {
        if (metadata.containsKey(fullAddress)) {
            BigInteger nodeToRemoveRangeStart = metadata.get(fullAddress).get(0);
            BigInteger nodeToRemoveRangeEnd = metadata.get(fullAddress).get(1);

            List<Map.Entry<String, List<BigInteger>>> sortedEntries = getSortedMetadata();
            BigInteger prevStart = BigInteger.ZERO;
            for (Map.Entry<String, List<BigInteger>> entry : sortedEntries) {
                String key = entry.getKey();
                BigInteger rangeStart = entry.getValue().get(0);
                int comparisonResult = rangeStart.compareTo(nodeToRemoveRangeStart);
                if (comparisonResult > 0) {
                    List<BigInteger> removedNodeRange = metadata.get(fullAddress);
                    metadata.remove(fullAddress);
                    logger.info(fullAddress + " removed from metadata.");
                    // extend the range of the successor node to cover removed node range
                    List<BigInteger> successorRange = entry.getValue();
                    successorRange.set(1, nodeToRemoveRangeEnd);
                    metadata.put(key, successorRange);
                    logger.info("update the range of the successor node " + key + " to " + successorRange);


                    successors.put(key, successors.get(fullAddress));
                    successors.remove(fullAddress);

                    predecessors.put(successors.get(key), key);
                    predecessors.remove(fullAddress);

                    return removedNodeRange;
                }
            }
        }
        return null;
    }

    // return sorted nodes in metadata map by the start of their key-range
    private List<Map.Entry<String, List<BigInteger>>> getSortedMetadata() {
        List<Map.Entry<String, List<BigInteger>>> sortedEntries = new ArrayList<>(metadata.entrySet());
        Collections.sort(sortedEntries, new Comparator<Map.Entry<String, List<BigInteger>>>() {
            @Override
            public int compare(Map.Entry<String, List<BigInteger>> e1, Map.Entry<String, List<BigInteger>> e2) {
                BigInteger first1 = e1.getValue().get(0);
                BigInteger first2 = e2.getValue().get(0);
                return first1.compareTo(first2);
            }
        });

        return sortedEntries;
    }

    /**
     * Put/Update an empty mailbox for a new/existing node
     * @param fullAddress serverIP:L-port
     * @return true for success, false otherwise
     */
    public boolean putEmptyMailbox(String fullAddress) {
        Mailbox newMailbox = new Mailbox();
        newMailbox.needsToSendWriteLock = false;
        newMailbox.valueSend_forWriteLock = null;
        childMailboxs.put(fullAddress, newMailbox);
        return true;
    }

    public void run() {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.print("ECSClient started.");
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                if (serverSocket != null) {
                    while (true) {
                        try {
                            // blocked until receiving a connection from a KVServer
                            Socket responseSocket = serverSocket.accept(); // a connection from the server
                            logger.info("Connected to "
                                    + responseSocket.getInetAddress().getHostName()
                                    + " on port " + responseSocket.getPort());
                            ECSClientChild childRunnable = new ECSClientChild(responseSocket, ECSClient.this);
                            Thread childThread = new Thread(childRunnable);
                            ChildObject childObject = new ChildObject();
                            // record response socket and child thread in a pair
                            childObject.childSocket = responseSocket;
                            childObject.childThread = childThread;
                            // map addr:port to runnable
                            childObjects.put(responseSocket.getInetAddress().getHostName() + ":" + responseSocket.getPort(), childObject);
                            // start the child thread
                            childThread.start();

                        } catch (IOException e) {
                            if (running) {
                                logger.warn("Warning! " +
                                        "Unable to establish connection. " +
                                        "Continue listening...\n" + e);
                                break;
                            } else {
                                logger.warn("Warning! " +
                                        "ECS no longer running, Shutting down.\n" + e);
                                closeServerSocket();
                            }

                        } catch (Exception e) {
                            logger.error("Unexpected Error! " +
                                    "Killing..." + e);
                            closeServerSocket();
                        }
                    }
                }
            }
        });

        thread1.start();
        thread2.start();

    }


    /**
     * Command line options
     */
    private static Options buildOptions() {
        //	Setting up command line params
        Options options = new Options();

        Option port = new Option("p", "port", true, "Sets the port of the server");
        port.setRequired(true);
        port.setType(Integer.class);
        options.addOption(port);

        Option address = new Option("a", "address", true, "Which address the server should listen to, default set to localhost");
        address.setRequired(false);
        address.setType(String.class);
        options.addOption(address);

        Option loglevel = new Option("ll", "loglevel", true, "Loglevel, e.g., INFO, ALL, …, default set to be ALL");
        loglevel.setRequired(false);
        loglevel.setType(String.class);
        options.addOption(loglevel);

        return options;
    }

    public static void main(String[] args) {
        try {
            Options options = buildOptions();

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            CommandLine cmd;

            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                formatter.printHelp("command", options);
                return;
            }

            // Initialize ecs client with command line params
            String address = cmd.getOptionValue("a", "localhost");
            System.out.println(address);
            int port = Integer.parseInt(cmd.getOptionValue("p"));

            ECSClient ecsClient = new ECSClient(address, port);

            String logLevelString = cmd.getOptionValue("ll", "ALL");
            new LogSetup("logs/ecs.log", Level.toLevel(logLevelString));

            ecsClient.initializeServer();
            ecsClient.run();
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);        }
    }
}
