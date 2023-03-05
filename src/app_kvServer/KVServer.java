package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.cli.*;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;


public class KVServer extends Thread implements IKVServer {
	/**
	 * Datatype containing a pair of response socket and thread.
	 */
	class ChildObject {
		public Thread childThread;
		public Socket childSocket;
	}

	private static Logger logger = Logger.getRootLogger();

	private SerStatus status;
	private int port;
	private String hostname;
	private Store store;
	private String bootstrapServer;
	// log file path and log level are not stored
	private ArrayList<ChildObject> childObjects;
	private Socket ECSSocket;
	private ServerSocket serverSocket;
	private OutputStream output;
	private InputStream input;
	// latest server metadata
    private String metadata;

	/**
	 * Initialize a new, stopped KVServer at port
	 * @param port 		port of server
	 */
	public KVServer(int port) {
		status = SerStatus.STOPPED;
		this.port = port;
		hostname = null;
		store = null;
		childObjects = null;
		serverSocket = null;
		metadata = null;
	}
	public KVServer(int port, int cacheSize, String strategy) {
		status = SerStatus.STOPPED;
		this.port = port;
		hostname = null;
		store = null;
		childObjects = null;
		serverSocket = null;
		metadata = null;
	}

	/* Server metadata */
	/**
	 * Set server's metadata,
	 * @param metatdata metadata from ECS server
	 * @return true for success, false otherwise
	 */
	public synchronized boolean setMetadata(String metatdata) {
		this.metadata = metatdata;
		return true;
	}
	/**
	 * Get server's latest metadata.
	 * @return
	 */
	public String getMetadata() {
		return metadata;
	}

	/* Server status enum */
	@Override
	public synchronized boolean setSerStatus(SerStatus status) {
		if ((this.status == SerStatus.SHUTTING_DOWN)
			&& (status != SerStatus.SHUTTING_DOWN)){
			logger.warn("Cancel setSerStatus as server #" + getPort()
						+ " is shutting down.");
			return false;
		} else if (this.status != status) {
			this.status = status;
			logger.info("Set server #" + getPort() + " status to "
						+ this.status.name());
		}
		return true;
	}
	@Override
	public SerStatus getSerStatus() {
		return status;
	}

	/* Port */
	@Override
	public int getPort(){
		return port;
	}

	/* Hostname */
	public boolean setHostname(String hostname) {
		if (this.hostname != null) {
			logger.error("Attempted to re-set existing server #" + getPort()
						+ " hostname.");
			return false;
		}
		this.hostname = hostname;
		logger.info("Server hostname: " + this.hostname + ", port: " + this.port);
		return true;
	}

	public void setBootstrapServer(String bootstrapServer) {
		this.bootstrapServer = bootstrapServer;
		return;
	}

	@Override
    public String getHostname(){
		return hostname;
	}

	/* Store */
	public boolean initializeStore(String dirStore) {
		if (store != null) {
			logger.error("Attempted to re-init existing server #" + getPort()
						+ " disk storage.");
			return false;
		}
		try {
			store = new Store(dirStore);
		} catch (Exception e) {
			logger.error("Exception when initializing server #" + getPort()
						+ " disk storage.", e);
			return false;
		}
		logger.info("Server #"+ port + " disk storage initialization successful.");
		return true;
	}
	@Override
    public boolean inStorage(String key) {
		if (store == null) {
			logger.error("Attempted to check key, when disk for server #" + getPort()
						+ " has NOT been initialized.");
			return false;
		}
		try {
			return store.containsKey(key);
		} catch (Exception e) {
			logger.error("Exception when checking if <" + key + "> in disk of server #"
						+ getPort() + ".", e);
			return false;
		}
	}
	@Override
    public boolean clearStorage() {
		if (store == null) {
			logger.error("Attempted to clear storage, when disk for server #" + getPort()
						+ " has NOT been initialized.");
			return false;
		}
		try {
			store.clearStorage();
		} catch (Exception e) {
			logger.error("Exception when clearing disk storage for server #" + getPort()
						+ ".");
			return false;
		}
		logger.info("Server #"+ port +" disk storage cleared.");
		return true;
	}

	/* Test method: get & put */
	@Override
    public String getKV(String key) throws Exception{
		try {
			// GET
			String val = store.get(key);
			if (val == null) {
				String keyNotFoundErr = "Error! Couldn't find key: " + key + " in store"
										+ " for server #" + getPort() + ".";
				System.err.println(keyNotFoundErr);
				logger.error(keyNotFoundErr);
				throw new Exception(keyNotFoundErr);
			}
			return val;
		} catch (Exception e) {
			String errMsg = "Error when fetching key: " + key + " from server #"
							+ getPort() + ".";
			System.err.println(errMsg);
			logger.error(errMsg, e);
			throw new Exception(errMsg);
		}
	}
	@Override
    public void putKV(String key, String value) throws Exception{
		String cmd_specific = "";
		try {
			if (value == null) {
				// DELETE
				cmd_specific = "(DELETE)";
				store.delete(key);
			} else if (store.containsKey(key)) {
				// UPDATE
				cmd_specific = "(UPDATE)";
				store.update(key, value);
			} else {
				// PUT
				cmd_specific = "(PUT)";
				store.put(key, value);
			}
		} catch (Exception e) {
			String errMsg = "Error when trying to add key-value pair on server #"
							+ getPort() + ". " + cmd_specific;
			System.err.println(errMsg);
			logger.error(errMsg, e);
			throw new Exception(errMsg);
		}
	}

	/* Server control */
	@Override
	// runs in the main thread for server
    public void run(){
		// main thread of the server
		connectECS();
		if (!initializeServer()) {
			logger.error("Server #" + getPort() + " initialization failed. "
						+ "Terminated.");
			return;
		}

		setSerStatus(SerStatus.RUNNING); 	// comment this line out when ECS is in control

		loop: while (true) {
			// catch all unexpected exceptions, only exit with break
			try {
				if (getSerStatus() == SerStatus.SHUTTING_DOWN) {
					// the last loop: shutting down server
					close();
					break loop;
				}
				// blocked until receiving a connection from client or ECS
				Socket responseSocket = serverSocket.accept();
				logger.info("Server #" + getPort() + " accepts a connection from"
							+ " socket at IP: "
							+ responseSocket.getInetAddress().getHostAddress()
							+ " and port: " + responseSocket.getPort());
				// run the communication module on a new child thread
				KVServerChild childRunnable = new KVServerChild(responseSocket, this);
				Thread childThread = new Thread(childRunnable);
				ChildObject childObject = new ChildObject();
				// record response socket and child thread in a pair
				childObject.childSocket = responseSocket;
				childObject.childThread = childThread;
				childObjects.add(childObject);
				// start the child thread
				childThread.start();
				// clean up child threads that have exited during the block.
				if (!cleanExitedChildThreads()) {
					continue;
				}
			} catch (IOException ioe) {
				// server listening socket is closed by others while in accept()
				logger.error("Server #" + getPort() + "'s listening socket was closed"
							+ " during accept(). "
							+ "Not an error if caused by manual interrupt", ioe);
				close();
				break loop;
			} catch (Exception e) {
				// report unexpected exception (method does not throw excep)
				logger.error("Server #" + getPort() + "encountered an unexpected "
							+ "exception in main thread run().", e);
				close();
				break loop;
			}
		}
		logger.info("Server #" + getPort() + " has shut down.");
		return;
	}

	public void connectECS() {
		try {
			ECSSocket = new Socket(String.valueOf(this.bootstrapServer.split(":")[0]), Integer.parseInt(this.bootstrapServer.split(":")[1]));
			output = ECSSocket.getOutputStream();
			input = ECSSocket.getInputStream();

			logger.info("ECS Connection established to: " + bootstrapServer);
		} catch (UnknownHostException e) {
			logger.error("Error! IP address for host '" + String.valueOf(this.bootstrapServer.split(":")[0])
					+ "' cannot be found.");
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket on host: '"
					+ bootstrapServer);
		}
	}

	/**
	 * Initialize server, check if all settings are given.
	 * @return true for success, false otherwise
	 */
	private boolean initializeServer() {
    	logger.info("Initialize server...");
		// Crtl+C shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				close();
				logger.debug("Shutdown hook thread exited.");
				return;
			}
		});
		// ensure status is STOPPED
		if (getSerStatus() != SerStatus.STOPPED) {
			logger.error("Cannot initialize with a run server.");
			return false;
		}
		// no need to check port, server initialized with port as name
		// check if hostname has been set
		if (getHostname() == null) {
			logger.error("Hostname not set. Call 'setHostname' once.");
			setSerStatus(SerStatus.SHUTTING_DOWN);
			return false;
		}
		// check if store (aka disk storage) has been initialized.
		if (store == null) {
			logger.error("Server store (disk storage) not initialized. "
						+ "Call 'initializeStore' once.");
			setSerStatus(SerStatus.SHUTTING_DOWN);
			return false;
		}
		// initialize client object(thread + socket) arraylist
		childObjects = new ArrayList<ChildObject>();

		// find IP for hostname
		InetAddress bindAddr;
		try {
			bindAddr = InetAddress.getByName(getHostname());
		} catch (UnknownHostException e) {
			logger.error("Exception! IP address for hostname '" + getHostname()
						 + "' cannot be found.", e);
			setSerStatus(SerStatus.SHUTTING_DOWN);
			return false;
		}
		// create socket with IP and port
    	try {
            serverSocket = new ServerSocket(getPort(), 0, bindAddr);
			serverSocket.setReuseAddress(true);
            logger.info("Server hostname: "
						+ serverSocket.getInetAddress().getHostName()
						+ " \tport: " + serverSocket.getLocalPort());
        } catch (IOException e) {
        	logger.error("Exception! Cannot open server socket at hostname: '"
						+ getHostname() + "' \tport: " + getPort());
			closeServerSocket();
			setSerStatus(SerStatus.SHUTTING_DOWN);
            return false;
        }
		return true;
    }

	/**
	 * Close the listening socket of the server.
	 * Should not be directly called in run(), as child sockets are not affected.
	 * Note: this function does not set SerStatus to SHUTTING_DOWN
	 */
	private void closeServerSocket() {
		if (serverSocket == null) {
			return;
		}
		if (serverSocket.isClosed()) {
			serverSocket = null;
			return;
		}
		try {
			logger.debug("Closing server #" + getPort()+ " socket...");
			serverSocket.close();
			serverSocket = null;
		} catch (Exception e) {
			logger.error("Unexpected Exception! Unable to close server socket"
						+ " at host: '" + getHostname()
						+ "' \tport: " + getPort(), e);
			// unsolvable error, the server must be shut down now.
			setSerStatus(SerStatus.SHUTTING_DOWN);
			serverSocket = null;
		}
	}

	@Override
    public synchronized void kill(){
		// immediately returns; should not call this, use close() instead.
		logger.debug("Server #" + getPort() + " is being KILLED.");
		setSerStatus(SerStatus.SHUTTING_DOWN);
        closeServerSocket();
		joinChildThreads(true);
	}

	@Override
    public synchronized void close(){
		logger.debug("Server #" + getPort() + " is shutting down.");
		// for internal shutdown, SerStatus should be SHUTTING_DOWN before this.
		setSerStatus(SerStatus.SHUTTING_DOWN);
		closeServerSocket();
		joinChildThreads(false);
	}

	/**
	 * Wait for all child threads to exit.
	 * Should not be directly called in run(), as other steps are required,
	 * including setting SHUTTING_DOWN status and closing listening socket.
	 * @param isKilling true for kill(), false for close()
	 */
	private void joinChildThreads(boolean isKilling) {
		if (childObjects == null) {
			return;
		}
		try{
			logger.debug("Waiting for server #" + getPort()
						+ "'s' child client threads to exit...");
			Iterator<ChildObject> iterObjects = childObjects.iterator();
			while (iterObjects.hasNext()) {
				ChildObject childObject = iterObjects.next();
				Thread childThread = childObject.childThread;
				Socket childSocket = childObject.childSocket;
				if (isKilling) {
					// for kill(),
					// absurdly interrupt child threads by closing their socket
					childSocket.close();
					childThread.join(100);
				} else {
					// for close(),
					// wait till they finish their response, then close
					childThread.join(900);
					childSocket.close();
					childThread.join(100);
				}
				iterObjects.remove();
				logger.debug("Server #" + getPort() + "'s child exited.");
			}
			logger.debug("All child client threads of server #" + getPort()
						+ " has exited.");
			childObjects = null;
		} catch (Exception e) {
			logger.error("Unexpected Exception! Server #" + getPort()
						+ " failed to wait for all child client threads"
						+ " to finish.", e);
			// unsolvable error, the server must be shut down now.
			setSerStatus(SerStatus.SHUTTING_DOWN);
			childObjects = null;
		}
	}

	/**
	 * Regularly call this method to remove exited threads from
	 * server's child client thread ArrayList.
	 * @return true for success, false otherwise.
	 */
	private boolean cleanExitedChildThreads() {
		if (childObjects == null) {
			return true;
		}
		try {

			Iterator<ChildObject> iterObjects = childObjects.iterator();
			while (iterObjects.hasNext()) {
				ChildObject childObject = iterObjects.next();
				Thread childThread = childObject.childThread;
				if (!childThread.isAlive()) {
					iterObjects.remove();
				}
			}
		} catch (Exception e) {
			logger.error("Unexpected Exception! Server #" + getPort()
						+ " failed to clean up exited child client threads", e);
			// unsolvable error, the server must be shut down now.
			setSerStatus(SerStatus.SHUTTING_DOWN);
			childObjects = null;
			return false;
		}
		return true;
	}

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

		Option bootstrapServer = new Option("b", "bootstrapServer", true, "Sets the bootstrap server which is the ECS");
		address.setRequired(true);
		address.setType(String.class);
		options.addOption(bootstrapServer);

		Option directory = new Option("d", "directory", true, "Directory for files (Put here the files you need to persist the data, the directory is created upfront and you can rely on that it exists)");
		directory.setRequired(true);
		directory.setType(String.class);
		options.addOption(directory);

		Option logfilePath = new Option("l", "logfilePath", true, "Relative path of the logfile, e.g., “echo.log”, default set to be current directory\n");
		logfilePath.setRequired(false);
		logfilePath.setType(String.class);
		options.addOption(logfilePath);

		Option loglevel = new Option("ll", "loglevel", true, "Loglevel, e.g., INFO, ALL, …, default set to be ALL");
		loglevel.setRequired(false);
		loglevel.setType(String.class);
		options.addOption(loglevel);

		return options;
	}

	/**
	 * Main entry point for the KVstore server application.
	 * @param args contains the port number at args[0].
	 */
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

			// arg 0: port
			KVServer server = new KVServer(Integer.parseInt(cmd.getOptionValue("p")));
			// arg 1: hostname
			server.setHostname(cmd.getOptionValue("a", "localhost"));
			// arg 2: directory of disk storage
			server.initializeStore(cmd.getOptionValue("d"));
			// arg 3: relative path of log file
			String pathLog = cmd.getOptionValue("l", "logs/server.log");
			// arg 4: Bootstrap ECS server
			server.setBootstrapServer(cmd.getOptionValue("b"));
			// arg 5: log level
			String logLevelString = cmd.getOptionValue("ll", "ALL");
			new LogSetup(pathLog, Level.toLevel(logLevelString));

			// call server.run() in its own thread
			server.start();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
