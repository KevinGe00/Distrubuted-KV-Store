package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Set;

public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;

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
			throw new Exception("Unknown host: " + address);
		} catch (IOException e) {
			throw new Exception("Could not establish a connection to: " + address + ":" + port);
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public void setRunning(boolean run) {
		this.running = run;
	}
}
