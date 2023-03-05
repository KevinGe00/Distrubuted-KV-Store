package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.Socket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;
import app_kvServer.IKVServer.SerStatus;

/**
 * A runnable class responsible to communicate with the client or ECS,
 * also process client's requests or ECS' commands.
 */
public class KVServerChild implements Runnable {
	private static Logger logger = Logger.getRootLogger();
	
	private Socket responseSocket;
	private String responseIP;
	private int responsePort;
	private KVServer ptrKVServer;
	private int serverPort;
	// note: DataInput/OutputStream does not need to be closed.
	private DataInputStream input;
	private DataOutputStream output;
	
	/**
	 * Construct a new child for communication and processing.
	 * @param responseSocket a response socket from accept() of server socket
	 * @param ptrKVServer the server object running on main thread
	 */
	public KVServerChild(Socket responseSocket, KVServer ptrKVServer) {
		this.responseSocket = responseSocket;
		responseIP = responseSocket.getInetAddress().getHostAddress();
		responsePort = responseSocket.getPort();
		this.ptrKVServer = ptrKVServer;
		serverPort = ptrKVServer.getPort();
	}
	
	/**
	 * Run in child thread. For communication and processing.
	 */
	public void run() {
		try {
			input = new DataInputStream(new BufferedInputStream(responseSocket.getInputStream()));
			output = new DataOutputStream(new BufferedOutputStream(responseSocket.getOutputStream()));
		} catch (Exception e) {
			logger.error("Failed to create data stream for child socket of server #"
						+ serverPort+ " connected to IP: '"+ responseIP + "' \t port: "
						+ responsePort, e);
			close();
			return;
		}

		while (ptrKVServer.getSerStatus() != SerStatus.SHUTTING_DOWN) {
			// === receive message ===
			KVMessage kvMsgRecv = null;
			try {
				// block until receiving response, exception when socket closed
				kvMsgRecv = receiveKVMessage();
			} catch (Exception e) {
				logger.error("Exception when receiving message at child socket of server #"
							+ serverPort+ " connected to IP: '"+ responseIP + "' \t port: "
							+ responsePort
							+ " Not an error if caused by manual interrupt.", e);
				close();
				return;
			}
			// =======================
			
			// === process received message, and reply ===
			StatusType statusRecv = kvMsgRecv.getStatus();
			String keyRecv = kvMsgRecv.getKey();
			String valueRecv = kvMsgRecv.getValue();
			// 1. server-status response for client request
			SerStatus serStatus = ptrKVServer.getSerStatus();
			if (statusRecv == StatusType.GET) {
				if ((serStatus == SerStatus.STOPPED)
					|| (serStatus == SerStatus.SHUTTING_DOWN)) {
					/*
					 * Server is stopped or shutting down,
					 * reply with 'server stopped' and full message received.
					 */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					kvMsgSend.setKey(keyRecv);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
			} else if (statusRecv == StatusType.PUT) {
				if ((serStatus == SerStatus.STOPPED)
					|| (serStatus == SerStatus.SHUTTING_DOWN)) {
					/*
					 * Server is stopped or shutting down,
					 * reply with 'server stopped' and full message received.
					 */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					kvMsgSend.setKey(keyRecv);
					if (valueRecv != null) {
						kvMsgSend.setValue(valueRecv);
					}
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				} else if (serStatus == SerStatus.WRITE_LOCK) {
					/*
					 * Server is locked for write, thus no PUT,
					 * reply with 'server stopped' and full message received.
					 */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_WRITE_LOCK);
					kvMsgSend.setKey(keyRecv);
					if (valueRecv != null) {
						kvMsgSend.setValue(valueRecv);
					}
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
			}
			// 2. normal KV response to client
			switch (statusRecv) {
				case GET: {
					/* GET */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (!ptrKVServer.inStorage(keyRecv)) {
						/*
						 * Key does not exist in storage,
						 * reply with 'get error'
						 */
						kvMsgSend.setStatus(StatusType.GET_ERROR);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					}
					try {
						if (!kvMsgSend.setValue(ptrKVServer.getKV(keyRecv))) {
							/*
							 * Value found cannot be sent,
							 * reply with 'get error'
							 */
							kvMsgSend.setStatus(StatusType.GET_ERROR);
							if(!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						}
						/*
						 * Value found,
						 * reply with 'get success'
						 */
						kvMsgSend.setStatus(StatusType.GET_SUCCESS);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					} catch (Exception e) {
						/*
						 * GET processing error,
						 * reply with 'get error'
						 */
						logger.error("GET processing error at child of server #"
									+ serverPort+ " connected to IP: '"
									+ responseIP + "' \t port: "+ responsePort, e);
						kvMsgSend.setStatus(StatusType.GET_ERROR);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					}
				}
				case PUT: {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (valueRecv == null) {
						/* DELETE */
						if (!ptrKVServer.inStorage(keyRecv)) {
							kvMsgSend.setStatus(statusRecv);
						}
					}
					break;
				}
				default: {
					logger.error("Invalid message <" + statusRecv.name()
								+ "> received at child socket of server #"
								+ serverPort+ " connected to IP: '"+ responseIP
								+ "' \t port: " + responsePort);
					close();
					return;
				}
			}
			// ===========================================
		}
		close();
		return;
	}

	/**
	 * Close response socket and change it to null.
	 */
	private void close() {
		if (responseSocket == null) {
			return;
		}
		if (responseSocket.isClosed()) {
			responseSocket = null;
			return;
		}
		try {
			logger.debug("Child of server #" + serverPort + " closing response socket"
						+ " connected to IP: '" + responseIP + "' \t port: "
						+ responsePort + ". \nExiting very soon.");
			responseSocket.close();
			responseSocket = null;
		} catch (Exception e) {
			logger.error("Unexpected Exception! Unable to close child socket of "
						+ "server #" + serverPort + " connected to IP: '" + responseIP
						+ "' \t port: " + responsePort + "\nExiting very soon.", e);
			// unsolvable error, thread must be shut down now
			responseSocket = null;
		}
	}

	/**
	 * Universal method to SEND a KVMessage via socket; will log the message.
	 * Does NOT throw an exception.
	 * @param kvMsg a KVMessage object
	 * @return true for success, false otherwise
	 */
	private boolean sendKVMessage(KVMessage kvMsg) {
		try {
			kvMsg.logMessageContent();
			byte[] bytes_msg = kvMsg.toBytes();
			// LV structure: length, value
			output.writeInt(bytes_msg.length);
			output.write(bytes_msg);
			logger.debug("#" + serverPort + "-" + responsePort + ": "
						+ "Sending 4(length int) " +  bytes_msg.length + " bytes.");
			output.flush();
		} catch (Exception e) {
			logger.error("Exception when server #" + serverPort + " sends to IP: '"
						+ responseIP + "' \t port: " + responsePort
						+ "\n Should close socket to unblock client's receive().", e);
			return false;
		}
		return true;
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
		logger.debug("#" + serverPort + "-" + responsePort + ": "
					+ "Receiving 4(length int) + " + size_bytes + " bytes.");
		byte[] bytes = new byte[size_bytes];
		input.readFully(bytes);
		if (!kvMsg.fromBytes(bytes)) {
			throw new Exception("#" + serverPort + "-" + responsePort + ": "
								+ "Cannot convert all received bytes to KVMessage.");
		}
		kvMsg.logMessageContent();
		return kvMsg;
	}
}
