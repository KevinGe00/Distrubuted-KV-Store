package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageObj;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Arrays;

public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners = new HashSet<>();

	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		try {
			clientSocket = new Socket(address, port);
			setRunning(true);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

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
			}
		} catch (IOException ioe) {
			logger.error("Error! Unable to tear down connection!", ioe);
		}
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	@Override
	public KVMessageObj put(String key, String value) throws Exception {
		byte[] msgBytes, b1, b2, b3, b4, b5;
		/* PUT */
		b1 = new byte[] {(byte) StatusType.PUT.ordinal()};
		b3 = key.getBytes();
		b2 = new byte[] {(byte) b3.length};
		b5 = (value + '\n').getBytes();
		b4 = new byte[] {(byte) ((b5.length >> 16) & 0xff),
						 (byte) ((b5.length >> 8) & 0xff),
						 (byte) (b5.length & 0xff)};
		msgBytes = ByteBuffer.allocate(b1.length +
										b2.length +
										b3.length +
										b4.length +
										b5.length).put(b1)
							   			 .put(b2)
										 .put(b3)
										 .put(b4)
										 .put(b5)
										 .array();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND PUT \t"
					+ key + " " 
					+ value);
		/* Receive PUT Success / Error */
		return receiveMessage();
	}

	@Override
	public KVMessageObj get(String key) throws Exception {
		byte[] msgBytes, b1, b2, b3;
		/* GET */
		b1 = new byte[] {(byte) StatusType.GET.ordinal()};
		b3 = (key + '\n').getBytes();
		b2 = new byte[] {(byte) b3.length};
		msgBytes = ByteBuffer.allocate(b1.length +
										b2.length +
										b3.length).put(b1)
							   			 .put(b2)
										 .put(b3)
										 .array();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND GET \t"
					+ key);
		/* Receive GET Success / Error */
		return receiveMessage();
	}

	public void setRunning(boolean run) {
		this.running = run;
	}

	public KVMessageObj receiveMessage() throws IOException {
		KVMessageObj kvMsg = new KVMessageObj();
		int BUFFER_SIZE = 1024;
		int DROP_SIZE = 1 + 1 + 20 + 3 + 120000;

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first 1 char - StatusType from stream */
		int statusIdx = input.read();
		StatusType statusMsg = StatusType.values()[statusIdx];
		kvMsg.setStatus(statusMsg);
		if (statusMsg == StatusType.GET_ERROR) {
			/* GET_ERROR */
			logger.info("RECEIVE GET_ERROR: \t" 
					+ kvMsg.getStatus());
			return kvMsg;
		}

		byte read = (byte) input.read();
		boolean reading = true;
		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* get key */
		switch (statusMsg) {
			case GET_SUCCESS:
				kvMsg.setValue(new String(Arrays.copyOfRange(msgBytes, 0, msgBytes.length)));
				logger.info("RECEIVE GET_SUCCESS: \t"
							+ kvMsg.getStatus() + " "
							+ kvMsg.getValue());
				break;
			case PUT_SUCCESS:
				kvMsg.setKey(new String(Arrays.copyOfRange(msgBytes, 0, msgBytes.length)));
				logger.info("RECEIVE PUT_SUCCESS: \t"
							+ kvMsg.getStatus() + " "
							+ kvMsg.getKey());
				break;
			case PUT_ERROR:
				kvMsg.setKey(new String(Arrays.copyOfRange(msgBytes, 0, msgBytes.length)));
				logger.info("RECEIVE PUT_ERROR: \t"
							+ kvMsg.getStatus() + " "
							+ kvMsg.getKey());
				break;
			default:
				break;
		}
		return kvMsg;
	}
}
