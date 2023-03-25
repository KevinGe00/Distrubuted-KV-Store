package app_kvECS;

import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;

import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.BufferedReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    public ConcurrentHashMap<String, ChildObject> childObjects;
    public ConcurrentHashMap<String, String> successors;
    public ConcurrentHashMap<String, String> predecessors;
    public ConcurrentHashMap<String, Mailbox> childMailboxs;

    public ConcurrentHashMap<BigInteger, IECSNode> getHashRing() {
        return hashRing;
    }

    private ConcurrentHashMap<BigInteger, IECSNode> hashRing = new ConcurrentHashMap<>();
    //mapping of KVServers (defined by their IP:Port) and the associated range of hash value
    private ConcurrentHashMap<String, List<BigInteger>> metadata = new ConcurrentHashMap<>();
    //mapping of KVServers (defined by their IP:Port) and the associated range (including the range of the replicas) of hash value
    private ConcurrentHashMap<String, List<BigInteger>> metadataWithReplicas = new ConcurrentHashMap<>();
    private ArrayList<Thread> clients = new ArrayList<Thread>();
    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;
    }
    public ConcurrentHashMap<String, List<BigInteger>> getMetadata() {
        return metadata;
    }

    public boolean getRunning(){
        return running;
    }
    /* Port */
    public ConcurrentHashMap<String, String> getSuccessors(){
        return successors;
    }

    public ConcurrentHashMap<String, String> getPredecessors(){
        return predecessors;
    }
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
        childObjects = new ConcurrentHashMap<>();

        successors = new ConcurrentHashMap<>();
        predecessors = new ConcurrentHashMap<>();

        childMailboxs = new ConcurrentHashMap<>();
    }

    /*
     * close the listening socket of the server.
     * should not be directly called in run().
     */
    public void closeServerSocket() {
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

    // Add this lock object as a class member
    private final Object replicaLock = new Object();
    /**
     * @param serverHost host of node being deleted
     * @param serverPort port of node being deleted
     */
    public void handleReplicaChangesAfterNodeRemoval (String serverHost, int serverPort) {
        synchronized (replicaLock) {
                // DN = node being deleted
                // At this point, DN's data has already been moved to coord folder of DN's pred
                // Happens in writeLockProcess of KVServerECS
            String fullAddress = serverHost + ":" + serverPort;
            IECSNode curr = hashRing.get(hash(fullAddress)); // DN
            IECSNode pred = hashRing.get(hash(predecessors.get(fullAddress)));
            IECSNode succ = hashRing.get(hash(successors.get(fullAddress)));
            IECSNode pred_pred = hashRing.get(hash(predecessors.get(predecessors.get(fullAddress))));
            IECSNode succ_succ = hashRing.get(hash(successors.get(successors.get(fullAddress))));

            String succ_dir = getParentPath(succ.getStoreDir());
            String succ_succ_dir = getParentPath(succ_succ.getStoreDir());
            String pred_dir = pred.getStoreDir();
            String pred_pred_dir = pred_pred.getStoreDir();


            // 1. Copy DN's succ's replica of DN's data into DN's succ's replica of DN's pred
            copyFolder(succ_dir + File.separator + serverPort, succ_dir + File.separator + pred.getNodePort());

            // 2. Delete DN's succ's replica of DN
            deleteFolder(succ_dir + File.separator + serverPort);

            // 3. Delete DN's succ's succ's replica of DN
            if (curr.getNodePort() != succ_succ.getNodePort()) {
                deleteFolder(succ_succ_dir + File.separator + serverPort);
            }

            // 4. Copy DN's pred pred as a replica in DN's succ
            if (curr.getNodePort() != pred_pred.getNodePort()) {
                copyFolder(pred_pred_dir, succ_dir + File.separator + pred_pred.getNodePort());
            }

            // 5. Copy DN's pred as a replica in DN's succ succ
            if (curr.getNodePort() != succ_succ.getNodePort()) {
                copyFolder(pred_dir, succ_succ_dir + File.separator + pred.getNodePort());
            }

            // 6. Finally, delete the entire folder of DN
            String curr_dir = getParentPath(curr.getStoreDir());
            deleteFolder(curr_dir);

            logger.info(fullAddress + " replication deletion completed.");
        }
    }
    /**
     * @param fullAddress address:port
     */
    public void replicateNewServer(String fullAddress) {
        synchronized (replicaLock){
            System.out.println("REPLICATE NEW SERVER, FULLADDRESS: " + fullAddress);
            IECSNode curr = hashRing.get(hash(fullAddress));

            IECSNode pred = hashRing.get(hash(predecessors.get(fullAddress)));
            IECSNode succ = hashRing.get(hash(successors.get(fullAddress)));
            IECSNode pred_pred = hashRing.get(hash(predecessors.get(predecessors.get(fullAddress))));
            IECSNode succ_succ = hashRing.get(hash(successors.get(successors.get(fullAddress))));

            String pred_port = Integer.toString(pred.getNodePort());
            String pred_pred_port = Integer.toString(pred_pred.getNodePort());
            String succ_dir = getParentPath(succ.getStoreDir());
            String succ_succ_dir = getParentPath(succ_succ.getStoreDir());
            String curr_dir = getParentPath(curr.getStoreDir());
            logger.debug("curr STORE DIR: " + curr.getStoreDir());
            logger.debug("Successor of " + fullAddress + " is " + successors.get(fullAddress));
            logger.debug("Predecessor of " + fullAddress + " is " + predecessors.get(fullAddress));

            logger.debug("Deleting  " + succ_dir + File.separator + pred_port);
            deleteFolder(succ_dir + File.separator + pred_port);
            logger.debug("Deleting  " + succ_succ_dir + File.separator + pred_port);
            deleteFolder(succ_succ_dir + File.separator + pred_port);
            logger.debug("Deleting  " + succ_dir + File.separator + pred_pred_port);
            deleteFolder(succ_dir + File.separator +  pred_pred_port);

            copyFolder(pred_pred.getStoreDir(), curr_dir + File.separator + pred_pred.getNodePort());
            copyFolder(pred.getStoreDir(), curr_dir + File.separator + pred.getNodePort());
            copyFolder(pred.getStoreDir(), succ_dir + File.separator + pred.getNodePort());
            copyFolder(curr.getStoreDir(), succ_dir + File.separator + curr.getNodePort());
            copyFolder(curr.getStoreDir(), succ_succ_dir + File.separator + curr.getNodePort());
        }
    }

    public static String getParentPath(String path) {
        int lastForwardSlashIndex = path.lastIndexOf('/');
        int lastBackwardSlashIndex = path.lastIndexOf('\\');
        int lastIndex = Math.max(lastForwardSlashIndex, lastBackwardSlashIndex);
        if (lastIndex > 0) {
            return path.substring(0, lastIndex);
        } else {
            return null;
        }
    }

    public static void deleteFolder(String source){

        File folder = new File(source);
        //Skip if server is deleting its non-existent replicas
        try {
            String canonicalPath = folder.getCanonicalPath();
            String[] pathSegments = canonicalPath.split("\\" + File.separator);
            int pathLength = pathSegments.length;
            if (pathLength >= 2 && pathSegments[pathLength - 1].equals(pathSegments[pathLength - 2])) {
                logger.debug("Skipping Delete, same replica shouldn't exist in same server, source is: " + source);
                return;
            }
        } catch (IOException e) {
            logger.debug("Failed to get canonical path");
            return;
        }

        File[] contents = folder.listFiles();
        if (contents != null) {
            for (File f : contents) {
                logger.debug("Current file: " + f.toPath());
                if (! Files.isSymbolicLink(f.toPath())) {
                    logger.debug("Deleting file: " + f.toPath());
                    deleteFolder(f.getPath());
                }
            }
        }
        if (folder.delete()) {
            logger.debug("DELETED: " + folder.toPath());
        }else{
            logger.debug("DELETE SCREWED UP");
        }

    }

    public static void copyFolder(String source, String destination){
        File sourceFolder = new File(source);
        File destinationFolder = new File(destination);
        // Do nothing if the source is itself
        if (getParentPath(source).equals(getParentPath(destination))) {
            return;
        }
        System.out.println("COPYING FROM " + source + " TO " + destination);

        if (!destinationFolder.exists()) {
            if (destinationFolder.mkdirs()) {
                ;
            } else {
                //System.out.println("Failed to create directory: " + destinationFolder);
                ;
            }
        }
        for (String child : sourceFolder.list()) {
            File sourceChild = new File(sourceFolder, child);
            File destinationChild = new File(destinationFolder, child);
            try {
                Files.copy(sourceChild.toPath(), destinationChild.toPath(), StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Copied: " + sourceChild.toPath() + " to " + destinationChild.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean addNewNode(String serverHost, int serverPort, String storeDir) {
        logger.info("Attempting to add new server " + serverHost + ":" + serverPort + " to ecs.");
        try {
            String fullAddress =  serverHost + ":" + serverPort;
            BigInteger position = addNewServerNodeToHashRing(serverHost, serverPort, storeDir);
            updateMetadataWithNewNode(fullAddress, position);
            updateMetadataWithReplicas(fullAddress, false);
            putEmptyMailbox(fullAddress);
            logger.info("Successfully added new server " + serverHost + ":" + serverPort + " to ecs.");
            return true;
        } catch (Exception e) {
            logger.error("Unsuccessfully added new server " + serverHost + ":" + serverPort + " to ecs.", e);
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
            // needs to happen before hashring, successors and predecessor maps are updated
            handleReplicaChangesAfterNodeRemoval(serverHost, serverPort);

            String removedNodeStoreDir = removeServerNodeFromHashRing(serverHost, serverPort);
            List<BigInteger> removedNodeRange = updateMetadataAfterNodeRemoval(fullAddress);
            updateMetadataWithReplicas(fullAddress, true);

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

    /**
     * @param fullAddress IP:port of newly added server
     * @param fullAddress IP:port of newly added server
     */
    public void updateMetadataWithReplicas(String fullAddress, boolean removing) {
        // at this point, updateMetadataWithNewNode or updateMetadataAfterNodeRemoval has already run

        if (metadata.size() <= 3) {
            // if only 3 or less nodes in ring, each node has copy of every single kv pair in the entire ring
            for (String key : metadata.keySet()) {
                List<BigInteger> keyrange = metadata.get(key);
                BigInteger range_start = keyrange.get(0);

                metadataWithReplicas.put(key, Arrays.asList(range_start, range_start.add(BigInteger.ONE)));
            }

            if (removing) {
                metadataWithReplicas.remove(fullAddress);
            }
            return;
        }
        BigInteger newNodeRangeEnd = metadata.get(fullAddress).get(1);

        String succAddr = successors.get(fullAddress);
        String succSuccAddr = successors.get(succAddr);
        BigInteger newNodeSuccRangeEnd = metadata.get(succAddr).get(1);
        BigInteger newNodeSuccSuccRangeEnd = metadata.get(succSuccAddr).get(1);

        String predAddr = predecessors.get(fullAddress);
        BigInteger newNodePredRangeEnd = metadata.get(predAddr).get(1);

        // "wide" hashrange includes servers own coordinator range plus the range that it's 2 predecessors cover

        // set new node's "wide" hashrange
        metadataWithReplicas.put(fullAddress, Arrays.asList(getPredPredRangeStart(fullAddress), newNodeRangeEnd));

        // set new node's pred "wide" hashrange
        metadataWithReplicas.put(predAddr, Arrays.asList(getPredPredRangeStart(predAddr), newNodePredRangeEnd));

        // set new node's succ "wide" hashrange
        metadataWithReplicas.put(succAddr, Arrays.asList(getPredPredRangeStart(succAddr), newNodeSuccRangeEnd));

        // set new node's succ succ "wide" hashrange
        metadataWithReplicas.put(succSuccAddr, Arrays.asList(getPredPredRangeStart(succSuccAddr), newNodeSuccSuccRangeEnd));

        if (removing) {
            metadataWithReplicas.remove(fullAddress);
        }
    }

    private BigInteger getPredPredRangeStart(String fullAddress) {
        List<BigInteger> predPredRange = metadata.get(predecessors.get(predecessors.get(fullAddress)));
        return predPredRange.get(0);

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
            BigInteger nodeToRemoveRangeEnd = metadata.get(fullAddress).get(1);

            String succAddr = successors.get(fullAddress);
            String predAddr = predecessors.get(fullAddress);

            List<BigInteger> newPredRange = metadata.get(predAddr);
            newPredRange.set(1, nodeToRemoveRangeEnd);
            metadata.put(predAddr, newPredRange);

            successors.put(predAddr, succAddr);
            successors.remove(fullAddress);

            predecessors.put(succAddr, predAddr);
            predecessors.remove(fullAddress);

            metadata.remove(fullAddress);
            logger.info(fullAddress + " removed from metadata.");
        }
        return null;
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
