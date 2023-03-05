package client;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;;

public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners = new HashSet<>();

	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	// note: DataInput/OutputStream does not need to be closed.
	private DataOutputStream output;
	private DataInputStream input;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		try {
			clientSocket = new Socket(address, port);
			clientSocket.setReuseAddress(true);
			setRunning(true);
			output = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));
			input = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));

			logger.info("Connection established to: " + address + ":" + port);
		} catch (UnknownHostException e) {
			throw new UnknownHostException("Unknown host: " + address);
		} catch (IOException e) {
			throw new IOException("Could not establish a connection to: " + address + ":" + port);
		}
	}

	@Override
	public void disconnect() {
		try {
			if (clientSocket != null) {
				input.close();
				output.close();
				clientSocket.close();
				clientSocket = null;
				logger.info("Client terminated the connection.");
			}
		} catch (IOException ioe) {
			logger.error("Error! Unable to tear down connection!", ioe);
		}
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.PUT)) {
			throw new Exception("PUT: cannot set KVMessage's status to PUT.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("PUT: cannot set KVMessage's key to <"
								+ key 
								+ ">.");
		}
		if (!kvMsg.setValue(value)) {
			throw new Exception("PUT: cannot set KVMessage's value to <"
								+ value
								+ ">.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.GET)) {
			throw new Exception("GET: cannot set KVMessage's status to GET.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("GET: cannot set KVMessage's key to <"
								+ key
								+ ">.");
		}
		if (!kvMsg.setValue(null)) {
			throw new Exception("GET: cannot set KVMessage's value to <null>.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	public void setRunning(boolean run) {
		this.running = run;
	}

	/**
	 * Universal method to SEND a KVMessage via socket; will log the message.
	 * @param kvMsg a KVMessage object
	 * @throws Exception throws exception if parsing or sending bytes failed.
	 */
	private void sendKVMessage(KVMessage kvMsg) throws Exception {
		kvMsg.logMessageContent();
		byte[] bytes_msg = kvMsg.toBytes();
		// LV structure: length, value
		output.writeInt(bytes_msg.length);
		output.write(bytes_msg);
		logger.debug("Sending 4(length int) + " +  bytes_msg.length + " bytes to server.");
		output.flush();
	}

	/**
	 * Universal method to RECEIVE a KVMessage via socket; will log the message.
	 * @return a KVMessage object
	 * @throws Exception throws exception as closing socket causes receive() to unblock.
	 */
	private KVMessage receiveKVMessage() throws Exception {
		KVMessage kvMsg = new KVMessage();
		// LV structure: length, value
		int size_bytes = input.readInt();
		logger.debug("Receiving 4(length int) + " + size_bytes + " bytes from server.");
		byte[] bytes = new byte[size_bytes];
		input.readFully(bytes);
		if (!kvMsg.fromBytes(bytes)) {
			throw new Exception("Cannot convert all received bytes to KVMessage.");
		}
		kvMsg.logMessageContent();
		return kvMsg;
	}
}
