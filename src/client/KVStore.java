package client;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

			logger.info("Connected to server #" + port);
		} catch (UnknownHostException e) {
			throw new UnknownHostException("Unknown host: " + address);
		} catch (IOException e) {
			throw new IOException("Cannot connect to server #" + port);
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
				logger.info("Disconnected from server #" + port);
			}
		} catch (IOException ioe) {
			logger.error("Cannot disconnect from server #" + port + " ", ioe);
		}
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.PUT)) {
			throw new Exception("PUT: Cannot set KVMessage's Status to 'PUT'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("PUT: Cannot set KVMessage's Key to '" + key + "'.");
		}
		if (!kvMsg.setValue(value)) {
			throw new Exception("PUT: Cannot set KVMessage's Value to '" + value + "'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/* M4 */
	public KVMessage table_put(String key, String row, String col, String value) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.TABLE_PUT)) {
			throw new Exception("TABLE_PUT: Cannot set KVMessage's Status to 'TABLE_PUT'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("TABLE_PUT: Cannot set KVMessage's Key to '" + key + "'.");
		}
		String content = row + System.lineSeparator()
						+ col + System.lineSeparator()
						+ value;
		if (!kvMsg.setValue(content)) {
			throw new Exception("TABLE_PUT: Cannot set KVMessage's Value to '" + content + "'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/* M4 */
	public KVMessage table_delete(String key, String row, String col) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.TABLE_DELETE)) {
			throw new Exception("TABLE_DELETE: Cannot set KVMessage's Status to 'TABLE_DELETE'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("TABLE_DELETE: Cannot set KVMessage's Key to '" + key + "'.");
		}
		String content = row + System.lineSeparator()
						+ col;
		if (!kvMsg.setValue(content)) {
			throw new Exception("TABLE_DELETE: Cannot set KVMessage's Value to '" + content + "'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/* M4 */
	public KVMessage table_get(String key, String row, String col) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.TABLE_GET)) {
			throw new Exception("TABLE_GET: Cannot set KVMessage's Status to 'TABLE_GET'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("TABLE_GET: Cannot set KVMessage's Key to '" + key + "'.");
		}
		String content = row + System.lineSeparator()
						+ col;
		if (!kvMsg.setValue(content)) {
			throw new Exception("TABLE_GET: Cannot set KVMessage's Value to '" + content + "'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/* M4 */
	public KVMessage table_select(String key, String cols_cond_linebreak) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.TABLE_SELECT)) {
			throw new Exception("TABLE_SELECT: Cannot set KVMessage's Status to 'TABLE_SELECT'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("TABLE_SELECT: Cannot set KVMessage's Key to '" + key + "'.");
		}
		if (!kvMsg.setValue(cols_cond_linebreak)) {
			throw new Exception("TABLE_SELECT: Cannot set KVMessage's Value to '" + cols_cond_linebreak + "'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.GET)) {
			throw new Exception("GET: Cannot set KVMessage's Status to 'GET'.");
		}
		if (!kvMsg.setKey(key)) {
			throw new Exception("GET: Cannot set KVMessage's Key to '" + key + "'.");
		}
		if (!kvMsg.setValue(null)) {
			throw new Exception("GET: Cannot set KVMessage's Value to 'null'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/**
	 * Send command 'keyrange' to a running server for latest keyrange metadata
	 * @return KVMessage containing the metadata, or message 'server stopped'
	 * @throws Exception throws exception if parsing or sending bytes failed.
	 */
	public KVMessage getKeyrange() throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.GET_KEYRANGE)) {
			throw new Exception("GET_KEYRANGE: Cannot set KVMessage's Status to "
								+ "'GET_KEYRANGE'.");
		}
		if (!kvMsg.setKey(null)) {
			throw new Exception("GET_KEYRANGE: Cannot set KVMessage's Key to 'null'.");
		}
		if (!kvMsg.setValue(null)) {
			throw new Exception("GET_KEYRANGE: Cannot set KVMessage's Value to 'null'.");
		}
		sendKVMessage(kvMsg);
		return receiveKVMessage();
	}

	/**
	 * Send command 'keyrange_read' to a running server for latest keyrange metadata
	 * @return KVMessage containing the metadata, or message 'server stopped'
	 * @throws Exception throws exception if parsing or sending bytes failed.
	 */
	public KVMessage getKeyrangeRead() throws Exception {
		KVMessage kvMsg = new KVMessage();
		if (!kvMsg.setStatus(StatusType.GET_KEYRANGE_READ)) {
			throw new Exception("GET_KEYRANGE_READ: Cannot set KVMessage's Status to "
								+ "'GET_KEYRANGE_READ'.");
		}
		if (!kvMsg.setKey(null)) {
			throw new Exception("GET_KEYRANGE_READ: Cannot set KVMessage's Key to 'null'.");
		}
		if (!kvMsg.setValue(null)) {
			throw new Exception("GET_KEYRANGE_READ: Cannot set KVMessage's Value to 'null'.");
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
		kvMsg.logMessageContent(false);
		byte[] bytes_msg = kvMsg.toBytes();
		// LV structure: length, value
		output.writeInt(bytes_msg.length);
		output.write(bytes_msg);
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
		byte[] bytes = new byte[size_bytes];
		input.readFully(bytes);
		if (!kvMsg.fromBytes(bytes)) {
			throw new Exception("Cannot convert all received bytes to a KVMessage.");
		}
		kvMsg.logMessageContent(true);
		return kvMsg;
	}

	// hash string to MD5 bigint
    private BigInteger hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes());
            byte[] digest = md.digest();
            return new BigInteger(1, digest);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }
}
