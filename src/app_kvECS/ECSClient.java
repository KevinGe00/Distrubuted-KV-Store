package app_kvECS;

import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ECSClient implements IECSClient {
    private static Logger logger = Logger.getRootLogger();
    private String address;
    private int port;
    private boolean stop = false;
    private boolean running = false;
    private ServerSocket serverSocket;
    private HashMap<BigInteger, IECSNode> hashRing = new HashMap<>();

    public ECSClient(String address, int port) {
        this.address = address;
        this.port = port;
    }

    private void initializeServer() {
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

    private void addNewServerNodeToHashRing(String serverHost, int serverPort) {
        try {
            String fullAddress =  serverHost + ":" + serverPort;

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(fullAddress.getBytes());
            byte[] digest = md.digest();
            BigInteger hashInt = new BigInteger(1, digest);

            // Create new ECSNode
            String nodeName = "Server:" + fullAddress;
            ECSNode node = new ECSNode(nodeName, serverHost, serverPort);

            hashRing.put(hashInt, node);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
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
            int port = Integer.parseInt(cmd.getOptionValue("p"));
            ECSClient ecsClient = new ECSClient(address, port);
            ecsClient.initializeServer();

            String logLevelString = cmd.getOptionValue("ll", "ALL");
            new LogSetup("logs/ecs.log", Level.toLevel(logLevelString));

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
