package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.math.BigInteger;

import org.apache.commons.cli.*;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;


public class KVServer extends Thread implements IKVServer {
	/**
	 * Datatype containing a pair of response socket and thread.
	 */
	class ChildObject {
		public Thread childThread = null;
		public Socket childSocket = null;
	}

	private static Logger logger = Logger.getRootLogger();

	private SerStatus status;
	private int port;
	private String hostname;
	private String bootstrapServer; 	// ECS
	private String dirStore;
	private Store store;
	// log file path and log level are not stored
	private ArrayList<ChildObject> childObjects;
	private ChildObject ECSObject; 		// ECS
	private ServerSocket serverSocket;
	// latest server metadata
    private String metadata;
	private BigInteger rangeFrom_responsible;
	private BigInteger rangeTo_responsible;

	/**
	 * Initialize a new, stopped KVServer at port
	 * @param port 		port of server
	 */
	public KVServer(int port) {
		status = SerStatus.STOPPED;
		this.port = port;
		hostname = null;
		bootstrapServer = null;
		dirStore = null;
		store = null;
		childObjects = null;
		ECSObject = null;
		serverSocket = null;
		metadata = null;
		rangeFrom_responsible = null;
		rangeTo_responsible = null;
	}
	public KVServer(int port, int cacheSize, String strategy) {
		status = SerStatus.STOPPED;
		this.port = port;
		hostname = null;
		bootstrapServer = null;
		dirStore = null;
		store = null;
		childObjects = null;
		ECSObject = null;
		serverSocket = null;
		metadata = null;
		rangeFrom_responsible = null;
		rangeTo_responsible = null;
	}

	/* Server metadata */
	/**
	 * Set server's metadata, will update the hasmap metadata as well
	 * @param metatdata metadata from ECS server
	 * @return true for success, false otherwise
	 */
	public synchronized boolean setMetadata(String metatdata) {
		if (metatdata == null) {
			logger.error("Must not set metadata to null for server #"
						+ getPort());
			return false;
		}
		this.metadata = metatdata;
		logger.debug("Server #" + getPort() + " has updated it keyrange"
					+ " metadata.");
		try {
			updateBoundResponsible(metatdata, port);
			logger.debug("Server #" + getPort() + "'s new range_from: <"
						+ rangeFrom_responsible.toString(16)
						+ "> \tnew range_to: <"
						+ rangeTo_responsible.toString(16) + ">");
		} catch (Exception e) {
			logger.error("Unable to update keyrange bound for server #"
						+ getPort() + ".", e);
			return false;
		}
		return true;
	}
	/**
	 * Get server's latest metadata.
	 * @return
	 */
	public String getMetadata() {
		return metadata;
	}
	/**
	 * Check if the server is responsible for this key
	 * @param key a String key
	 * @return true for responsible, false otherwise and need to send SERVER_NOT_RESPONSIBLE
	 */
	public boolean isResponsibleToKey(String key) {
		BigInteger hashedKey = hash(key);
		return isBounded(hashedKey, rangeFrom_responsible, rangeTo_responsible);
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
	// check BigInteger within bound or not
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
     * Update rangeFrom_responsible and rangeTo_responsible with string metadata.
     * @param str a String keyrange metadata from ECS
     */
    public synchronized void updateBoundResponsible(String str, int responsePort) throws Exception {
        if ((str == null) || ("".equals(str))) {
            return;
        }
		// check if this regular metadata update or write lock keyrange update
		String[] semicolonSeperatedStrs = str.split(";");
		String[] commaSeperatedStrs = semicolonSeperatedStrs[0].split(",");
		if (commaSeperatedStrs.length == 5) {
			// write lock keyrange update
			// -> Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port
			rangeFrom_responsible = new BigInteger(commaSeperatedStrs[1], 16);
			rangeTo_responsible = new BigInteger(commaSeperatedStrs[2], 16);
			logger.info("WL keyrange update." + rangeFrom_responsible + "<->" + rangeTo_responsible);
		} else if (commaSeperatedStrs.length == 4) {
			// regular metadata update
			// -> range_from,range_to,ip,port;...;range_from,range_to,ip,port
			for (String pair : semicolonSeperatedStrs) {
				// range_from,range_to,ip,port
				String[] rF_rT_ip_port = pair.split(",");
				if (Integer.parseInt(rF_rT_ip_port[3]) == responsePort) {
					// find the pair for this server
					rangeFrom_responsible = new BigInteger(rF_rT_ip_port[0], 16);
					rangeTo_responsible = new BigInteger(rF_rT_ip_port[1], 16);
					logger.info("metadata keyrange update " + rangeFrom_responsible + "<->" + rangeTo_responsible);
					return;
				}
			}
		}
		return;
    }

	/* Server status enum */
	@Override
	public boolean setSerStatus(SerStatus status) {
		if (this.status != status) {
			logger.debug(">>> Set #" + getPort() + " to " + status.name());
			this.status = status;
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
	@Override
    public String getHostname(){
		return hostname;
	}

	/* Bootstrap ECS Server */
	public boolean setBootstrapServer(String bootstrapServer) {
		this.bootstrapServer = bootstrapServer;
		return true;
	}
	public String getBootstrapServer() {
		return bootstrapServer;
	}
	public String getECSHostname() {
		if (bootstrapServer == null) {
			logger.warn("Getting ECS hostname from null bootstrap "
						+ "in server #" + getPort());
			return null;
		}
		return String.valueOf(bootstrapServer.split(":")[0]);
	}
	public int getECSPort() {
		if (bootstrapServer == null) {
			logger.warn("Getting ECS port from null bootstrap "
						+ "in server #" + getPort());
			return 0;
		}
		return Integer.parseInt(bootstrapServer.split(":")[1]);
	}

	/* Store */
	public boolean setDirStore(String dirStore) {
		this.dirStore = dirStore;
		return true;
	}
	public String getDirStore() {
		return dirStore;
	}
	public boolean initializeStore(String dirStore) {
		if (store != null) {
			logger.info("Attempted to re-init existing server #" + getPort()
						+ " disk storage.");
			return false;
		}
		try {
			setDirStore(dirStore);
			store = new Store(port, port, dirStore);
		} catch (Exception e) {
			logger.error("Exception when initializing server #" + getPort()
						+ " disk storage.", e);
			return false;
		}
		logger.info("Server #"+ port + " disk storage initialization successful.");
		return true;
	}
	public boolean reInitializeStore() {
		try {
			store = new Store(port, port, dirStore);
		} catch (Exception e) {
			logger.error("Exception when re-initializing server #" + getPort()
						+ " disk storage.", e);
			return false;
		}
		logger.info("Server #"+ port + " disk storage re-initialization successful.");
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

	/* ECS Object: Socket + Thread */
	/**
	 * Single-use set ECS socket.
	 * @param ECSSocket a new socket connected to ECS
	 * @return true for success, false otherwise
	 */
	public boolean setECSSocket(Socket ECSSocket) {
		if (ECSObject == null) {
			logger.error("Attempted to set ECS socket, when ECS Object for server #" + getPort()
						+ " has NOT been initialized.");
			return false;
		}
		if (ECSObject.childSocket != null) {
			logger.error("Attempted to set ECS socket, when ECS socket for server #" + getPort()
						+ " already exists. 'setECSSocket' must be single-use.");
			return false;
		}
		ECSObject.childSocket = ECSSocket;
		return true;
	}
	public Socket getECSocket() {
		if (ECSObject == null) {
			return null;
		}
		if (ECSObject.childSocket == null) {
			return null;
		}
		return ECSObject.childSocket;
	}
	/**
	 * Single-use set ECS thread.
	 * @param ECSThread a new Thread where the communication module between server and ECS runs omn
	 * @return true for success, false otherwise
	 */
	public boolean setECSThread(Thread ECSThread) {
		if (ECSObject == null) {
			logger.error("Attempted to set ECS Thread, when ECS Object for server #" + getPort()
						+ " has NOT been initialized.");
			return false;
		}
		if (ECSObject.childThread != null) {
			logger.error("Attempted to set ECS thread, when ECS thread for server #" + getPort()
						+ " already exists. 'setECSThread' must be single-use.");
			return false;
		}
		ECSObject.childThread = ECSThread;
		return true;
	}
	public Thread getECSThread() {
		if (ECSObject == null) {
			return null;
		}
		if (ECSObject.childThread == null) {
			return null;
		}
		return ECSObject.childThread;
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
		if (!initializeServer()) {
			logger.error("Server #" + getPort() + " initialization failed. "
						+ "Terminated.");
			return;
		}

		// create a ECS connection socket and run communication in its own thread.
		if (!startServerECSThread()) {
			logger.error("Server #" + getPort() + " starting Server ECS thread failed. "
						+ "Terminated.");
			return;
		}

		// setSerStatus(SerStatus.RUNNING); 	// comment this line out when ECS has been implemented

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
							+ "Not an error if caused by manual interrupt.");
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

	/**
	 * Initialize server, check if all settings are given.
	 * @return true for success, false otherwise
	 */
	private boolean initializeServer() {
    	logger.info("Initializing server #" + port + "...");
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
		if ((getDirStore() == null) || (store == null)) {
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
			setSerStatus(SerStatus.SHUTTING_DOWN);
			closeServerSocket();
            return false;
        }
		return true;
    }

	/**
	 * Create a ECS connection socket, and run communication in its own thread
	 * @return true for success, false otherwise
	 */
	public boolean startServerECSThread() {
		if (getBootstrapServer() == null) {
			logger.error("Server bootstrap address not set. "
						+ "Call 'setBootstrapServer'.");
			setSerStatus(SerStatus.SHUTTING_DOWN);
			closeServerSocket();
			return false;
		}
		try {
			ECSObject = new ChildObject();
			setECSSocket(new Socket(getECSHostname(), getECSPort()));
			getECSocket().setReuseAddress(true);
			logger.info("Server #" + getPort() + " ECS connection established to: "
						+ bootstrapServer);
			// run the ECS communication module on a new child thread
			KVServerECS ecsRunnable = new KVServerECS(getECSocket(), this);
			setECSThread(new Thread(ecsRunnable));
			// start the child thread
			getECSThread().start();
		} catch (UnknownHostException e) {
			logger.error("Server #" + getPort() + " ECS IP address for host '" 
						+ getECSHostname() + "' cannot be found.");
			setSerStatus(SerStatus.SHUTTING_DOWN);
			closeServerSocket();
			joinECSThread(true);
			return false;
		} catch (IOException e) {
			logger.error("Server #" + getPort() + " cannot open ECS socket on: '"
						+ bootstrapServer);
			setSerStatus(SerStatus.SHUTTING_DOWN);
			closeServerSocket();
			joinECSThread(true);
			return false;
		} catch (Exception e) {
			logger.error("Server #" + getPort() + " starting ECS thread failed.", e);
			setSerStatus(SerStatus.SHUTTING_DOWN);
			closeServerSocket();
			joinECSThread(true);
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
		logger.debug("Commanding #" + getPort() + " to be KILLED.");
		setSerStatus(SerStatus.SHUTTING_DOWN);
		joinECSThread(true);
		closeServerSocket();
		joinChildThreads(true);
	}

	@Override
    public synchronized void close(){
		logger.info("Commanding #" + getPort() + " to shutdown.");
		// for internal shutdown, SerStatus should be SHUTTING_DOWN before this.
		setSerStatus(SerStatus.SHUTTING_DOWN);
		joinChildThreads(false);
		closeServerSocket();
		joinECSThread(false);
		logger.debug("Server close() complete.");
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
	private void joinECSThread(boolean isKilling) {
		if (ECSObject == null) {
			return;
		}
		if (getECSThread() == null) {
			closeECSSocket();
			ECSObject = null;
			return;
		}
		try{
			logger.debug("Waiting for server #" + getPort()
						+ "'s' ECS thread to exit...");
			Thread ecsThread = getECSThread();
			if (isKilling) {
				// for kill(),
				// absurdly interrupt child threads by closing their socket
				closeECSSocket();
				ecsThread.join(100);
			} else {
				// for close(): must let ECS complete all the works
				ecsThread.join();
				closeECSSocket();
			}

			logger.debug("Server #" + getPort() + "'s ECS exited.");
			ECSObject.childSocket = null;
			ECSObject.childThread = null;
			ECSObject = null;
		} catch (Exception e) {
			logger.error("Unexpected Exception! Server #" + getPort()
						+ " failed to wait for ECS thread to finish.", e);
			// unsolvable error, the server must be shut down now.
			setSerStatus(SerStatus.SHUTTING_DOWN);
			ECSObject = null;
		}
	}

	/**
	 * Close the connection socket of the ECS server.
	 * Should be called in joinECSThread().
	 * Note: this function does not set SerStatus to SHUTTING_DOWN
	 */
	private void closeECSSocket() {
		if (ECSObject == null) {
			return;
		}
		Socket sock = getECSocket();
		if (sock == null) {
			return;
		}
		if (sock.isClosed()) {
			ECSObject.childSocket = null;
			return;
		}
		try {
			logger.debug(">>> Closing #" + getPort()+ "'s ECS-related socket...");
			sock.close();
			ECSObject.childSocket = null;
		} catch (Exception e) {
			logger.error("Cannot close  ECS socket"
					+ " at host: '" + getECSHostname()
					+ "' \tport: " + getECSPort(), e);
			// unsolvable error, the server must be shut down now.
			setSerStatus(SerStatus.SHUTTING_DOWN);
			ECSObject.childSocket = null;
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

	private final Object moveFileLock = new Object();
	/**
	 * Transfer all KV pairs that hash to a specific keyrange to a new specified directory, 
	 * this server is responsible to send a notice to ECS when the file transfer finishes.
	 * In addition, it is responsible to send the server that receives the file a notice to release Write Lock.
	 */
    public void moveFilesTo(String dirNewStore, List<BigInteger> range) {
		synchronized (moveFileLock) {
			SerStatus serStatus = getSerStatus();
			if (serStatus == SerStatus.RUNNING) {
				setSerStatus(SerStatus.WRITE_LOCK);
			}
			String sourceFolder = dirStore;
			String destinationFolder = dirNewStore;

			File srcFolder = new File(sourceFolder);
			File destFolder = new File(destinationFolder);

			File[] files = srcFolder.listFiles();

			BigInteger range_from = range.get(0);
			BigInteger range_to = range.get(1);
			// Copy each files with range the source folder to the destination folder
			for (File file : files) {
				try {
					BigInteger hashKey = hash(file.getName());

					if (isBounded(hashKey, range_from, range_to)) {
						Path srcPath = file.toPath();
						Path destPath = new File(destFolder, file.getName()).toPath();
						logger.debug("Moving file " + file.getName() + " to " + destFolder.getAbsolutePath());
						Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
						if (Files.exists(destPath) && Files.size(srcPath) == Files.size(destPath)) {
							file.delete();
							logger.debug("Finished deleting.");
						} else {
							logger.error("Failed to copy file " + file.getName() + " to " + destFolder.getAbsolutePath() + " BECAUSE DESTINATION PATH DOESNT EXIST OR THAT THE COPIED FILE SIZE IS DIFFERENT.");
						}
					}
				} catch (IOException e) {
					logger.error("Server #" + getPort() + " failed to move file " + file.getName()
							+ " due to " + e.getClass().getName());
				}
			}
			setSerStatus(serStatus);
		}
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

		Option logfilePath = new Option("l", "logfilePath", true, "Relative path of the logfile, e.g., “echo.log”, default set to be current directory\n");
		logfilePath.setRequired(false);
		logfilePath.setType(String.class);
		options.addOption(logfilePath);

		// Optional
		Option strategy = new Option("s", "strategy", true, "Caching strategy\n");
		strategy.setRequired(false);
		strategy.setType(String.class);
		options.addOption(strategy);

		// Optional
		Option cacheSize = new Option("c", "cacheSize", true, "Size of cache\n");
		cacheSize.setRequired(false);
		cacheSize.setType(String.class);
		options.addOption(cacheSize);

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
			server.initializeStore("out" + File.separator + cmd.getOptionValue("p") + File.separator + "Coordinator");
			// arg 3: relative path of log file
			String pathLog = cmd.getOptionValue("l", "logs/server.log");
			// arg 4: Bootstrap ECS server
			server.setBootstrapServer(cmd.getOptionValue("b"));
			// arg 5: log level
			String logLevelString = cmd.getOptionValue("ll", "ALL");
			// arg 6: cache strategy
			String strategy = cmd.getOptionValue("s", "FIFO"); // unused atm
			// arg 7: log level
			String cacheSize = cmd.getOptionValue("c", "1"); // unused atm
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
