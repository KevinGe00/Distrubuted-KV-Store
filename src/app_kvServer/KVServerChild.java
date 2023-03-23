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
			// get server status
			SerStatus serStatus = ptrKVServer.getSerStatus();
			// 0. command-type response (client's GET_KEYRANGE, and ECS' commands)
			if (statusRecv == StatusType.GET_KEYRANGE) {
				/* KEYRANGE command */
				if ((serStatus == SerStatus.STOPPED) || (serStatus == SerStatus.SHUTTING_DOWN)) {
					KVMessage kvMsgSend = new KVMessage();
					/*
					 * Client commands 'keyrange', but server is stopped or shutting down,
					 * reply with 'server stopped', as specified in the module
					 */
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
				KVMessage kvMsgSend = new KVMessage();
				kvMsgSend.setValue(ptrKVServer.getMetadata());
				/*
				 * Client commands 'keyrange', server is not stopped or shutting down,
				 * reply with 'keyrange success' and latest server metadata
				 */
				kvMsgSend.setStatus(StatusType.KEYRANGE_SUCCESS);
				if (!sendKVMessage(kvMsgSend)) {
					close();
					return;
				}
				continue;
			} else if (statusRecv == StatusType.GET_KEYRANGE_READ) {
				/* KEYRANGE_READ command */
				if ((serStatus == SerStatus.STOPPED) || (serStatus == SerStatus.SHUTTING_DOWN)) {
					KVMessage kvMsgSend = new KVMessage();
					/*
					 * Client commands 'keyrange', but server is stopped or shutting down,
					 * reply with 'server stopped', as specified in the module
					 */
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
				KVMessage kvMsgSend = new KVMessage();
				kvMsgSend.setValue(ptrKVServer.getMetadata());
				/*
				 * Client commands 'keyrange', server is not stopped or shutting down,
				 * reply with 'keyrange success' and latest server metadata
				 */
				kvMsgSend.setStatus(StatusType.KEYRANGE_SUCCESS);
				if (!sendKVMessage(kvMsgSend)) {
					close();
					return;
				}
				continue;
			} else if (statusRecv == StatusType.S2A_FINISHED_FILE_TRANSFER) {
				/* SPECIAL CASE: this server just received KV pairs in its disk storage. */
				if (Integer.parseInt(keyRecv) == serverPort) {
					// sender is youself, can happen when you are the last node shutting down
					close();
					return;
				}
				SerStatus serStatus_Copy = serStatus;
				if ((serStatus == SerStatus.RUNNING)
					|| (serStatus == SerStatus.WRITE_LOCK)) {
					// temporarily stop this server as it will re-initialize store
					ptrKVServer.setSerStatus(SerStatus.STOPPED);
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				if (!ptrKVServer.reInitializeStore()) {
					/* Fatal error, the shared Store cannot be re-initialized. */
					ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
				} else {
					ptrKVServer.setSerStatus(serStatus_Copy);
				}
				// one-time connection.
				close();
				return;
			}

			// 1. server-status response for client request
			if (statusRecv == StatusType.GET) {
				if ((serStatus == SerStatus.STOPPED)
					|| (serStatus == SerStatus.SHUTTING_DOWN)) {
					/*
					 * Server is stopped or shutting down,
					 * reply GET with 'server stopped' and full GET message.
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
					 * reply PUT with 'server stopped' and full PUT message.
					 */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					kvMsgSend.setKey(keyRecv);
					kvMsgSend.setValue(valueRecv);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				} else if (serStatus == SerStatus.WRITE_LOCK) {
					/*
					 * Server is locked for write, thus no PUT,
					 * reply PUT with 'server stopped' and full PUT message.
					 */
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_WRITE_LOCK);
					kvMsgSend.setKey(keyRecv);
					kvMsgSend.setValue(valueRecv);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
			}

			// 2. server not responsible
			if ((statusRecv == StatusType.PUT)
				|| (statusRecv == StatusType.GET)) {
				if (!ptrKVServer.isResponsibleToKey(keyRecv)) {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setValue(ptrKVServer.getMetadata());
					/*
					 * This server is not responsible for this key,
					 * reply with latest metadata.
					 */
					kvMsgSend.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					if (!sendKVMessage(kvMsgSend)) {
						close();
						return;
					}
					continue;
				}
			}
			
			// 3. normal KV response to client
			switch (statusRecv) {
				case GET: {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (!ptrKVServer.inStorage(keyRecv)) {
						/*
						 * Key does not exist in storage,
						 * reply GET with 'get error' and Key
						 */
						kvMsgSend.setStatus(StatusType.GET_ERROR);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					}
					try {
						String valueSend = ptrKVServer.getKV(keyRecv);
						kvMsgSend.setValue(valueSend);
						/*
						 * Value found,
						 * reply GET with 'get success' and Key-Value pair
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
						 * reply with 'get error' and Key
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
					/* DELETE */
					if (valueRecv == null) {
						if (!ptrKVServer.inStorage(keyRecv)) {
							/*
							 * KV pair requested to delete does not exist,
							 * reply PUT(DELETE) with 'delete success' and Key
							 */
							kvMsgSend.setStatus(StatusType.DELETE_SUCCESS);
							if (!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						}
						try {
							String valueSend = ptrKVServer.getKV(keyRecv);
							kvMsgSend.setValue(valueSend);
							ptrKVServer.putKV(keyRecv, null);
							/*
							 * KV pair requested to delete existed and has been deleted,
							 * reply PUT(DELETE) with 'delete success' and KV pair
							 */
							kvMsgSend.setStatus(StatusType.DELETE_SUCCESS);
							if (!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						} catch (Exception e) {
							/*
							 * PUT(DELETE) processing error,
							 * reply with 'delete error' and Key (or KV pair)
							 */
							logger.error("PUT(DELETE) processing error at child of "
										+ "server #" + serverPort+ " connected to IP: '"
										+ responseIP + "' \t port: "+ responsePort, e);
							kvMsgSend.setStatus(StatusType.DELETE_ERROR);
							if (!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						}
					}
					/* UPDATE */
					if (ptrKVServer.inStorage(keyRecv)) {
						try {
							kvMsgSend.setValue(valueRecv);
							ptrKVServer.putKV(keyRecv, valueRecv);
							/*
							 * Key exists in storage and has been updated with new Value,
							 * reply PUT(UPDATE) with 'put update' and new KV pair
							 */
							kvMsgSend.setStatus(StatusType.PUT_UPDATE);
							if (!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						} catch (Exception e) {
							/*
							 * PUT(UPDATE) processing error,
							 * reply with 'put error' and KV pair
							 */
							logger.error("PUT(UPDATE) processing error at child of "
										+ "server #" + serverPort+ " connected to IP: '"
										+ responseIP + "' \t port: "+ responsePort, e);
							kvMsgSend.setStatus(StatusType.PUT_ERROR);
							if (!sendKVMessage(kvMsgSend)) {
								close();
								return;
							}
							continue;
						}
					}
					/* PUT */
					try {
						kvMsgSend.setValue(valueRecv);
						ptrKVServer.putKV(keyRecv, valueRecv);
						/*
						 * KV pair has been put into storage,
						 * reply PUT with 'put success' and the same KV pair
						 */
						kvMsgSend.setStatus(StatusType.PUT_SUCCESS);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					} catch (Exception e) {
						/*
						 * PUT processing error,
						 * reply with 'put error' and KV pair
						 */
						logger.error("PUT processing error at child of "
									+ "server #" + serverPort+ " connected to IP: '"
									+ responseIP + "' \t port: "+ responsePort, e);
						kvMsgSend.setStatus(StatusType.PUT_ERROR);
						if (!sendKVMessage(kvMsgSend)) {
							close();
							return;
						}
						continue;
					}
				}
				default: {
					logger.error("Invalid message <" + statusRecv.name()
								+ "> received at child socket of server #"
								+ serverPort + " connected to IP: '"+ responseIP
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
			kvMsg.logMessageContent(false);
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
		kvMsg.logMessageContent(true);
		return kvMsg;
	}
}
