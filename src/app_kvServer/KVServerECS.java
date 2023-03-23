package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;
import app_kvServer.IKVServer.SerStatus;

/**
 * A runnable class responsible to communicate with the client or ECS,
 * also process client's requests or ECS' commands.
 */
public class KVServerECS implements Runnable {
	private static Logger logger = Logger.getRootLogger();
	
	private Socket ecsSocket;
	private String ecsIP;
	private int ecsPort;
	private KVServer ptrKVServer;
	private int serverPort;
	// note: DataInput/OutputStream does not need to be closed.
	private DataInputStream input;
	private DataOutputStream output;
	
	public KVServerECS(Socket ecsSocket, KVServer ptrKVServer) {
		this.ecsSocket = ecsSocket;
		ecsIP = ecsSocket.getInetAddress().getHostAddress();
		ecsPort = ecsSocket.getPort();
		this.ptrKVServer = ptrKVServer;
		serverPort = ptrKVServer.getPort();
	}
	
	/**
	 * Run in Server's child ECS thread. For communication and processing.
	 */
	public void run() {
		try {
			input = new DataInputStream(new BufferedInputStream(ecsSocket.getInputStream()));
			output = new DataOutputStream(new BufferedOutputStream(ecsSocket.getOutputStream()));
		} catch (Exception e) {
			logger.error("Failed to create data stream for ECS child socket of server #"
						+ serverPort+ " connected to IP: '"+ ecsIP + "' \t port: "
						+ ecsPort + "|ECS", e);
			close();
			return;
		}

		// initialization: init request with dir
		logger.info("Started initialization communication between server #" + serverPort
					+ " and the ECS at IP: '" + ecsIP + "' \t port: " + ecsPort + "|ECS.");
		KVMessage kvMsgSend = new KVMessage();
		kvMsgSend.setStatus(StatusType.S2E_INIT_REQUEST_WITH_DIR);
		kvMsgSend.setKey(Integer.toString(serverPort)); 	// server's listening port, for moving files
		kvMsgSend.setValue(ptrKVServer.getDirStore()); 		// server's store directory
		if (!sendKVMessage(kvMsgSend, output)) {
			close();
			return;
		}
		// expect: init response with meta
		SerStatus serStatus = null;
		KVMessage kvMsgRecv = null;
		StatusType statusRecv = null;
		String keyRecv = null;
		String valueRecv = null;
		try {
			kvMsgRecv = receiveKVMessage(input);
		} catch (Exception e) {
			logger.error("Exception when receiving message at ECS child socket of server #"
						+ serverPort+ " connected to IP: '"+ ecsIP + "' \t port: "
						+ ecsPort + "|ECS. Not an error if caused by manual interrupt.", e);
			close();
			return;
		}
		statusRecv = kvMsgRecv.getStatus();
		if (statusRecv == StatusType.E2S_INIT_RESPONSE_WITH_META) {
			valueRecv = kvMsgRecv.getValue(); 	// keyrange metadata
			if (!ptrKVServer.setMetadata(valueRecv)) {
				close();
				return;
			}
		} else {
			// unexpected message
			logger.error("Not expected initialization response from ECS for server #"
						+ serverPort + " connected to IP: '" + ecsIP + "'\t port: "
						+ ecsPort + "|ECS. Message received was " + statusRecv.name());
			close();
			return;
		}
		logger.info("Server #" + serverPort + " finished initialization process with ECS at '"
                    + ecsIP + "' \t port: " + ecsPort + "|ECS.");

		loop: while (true) {
			// receive message from ECS. (ECS sends regular check to server.)
			try {
				kvMsgRecv = receiveKVMessage(input);
			} catch (Exception e) {
				logger.error("Exception when receiving message at ECS child socket of server #"
							+ serverPort+ " connected to IP: '"+ ecsIP + "' \t port: "
							+ ecsPort + "|ECS. Not an error if caused by manual interrupt.", e);
				close();
				return;
			}
			
			// === process received message, and reply ===
			statusRecv = kvMsgRecv.getStatus();
			keyRecv = kvMsgRecv.getKey();
			valueRecv = kvMsgRecv.getValue();
			// get server status
			serStatus = ptrKVServer.getSerStatus();

			// pre-prepare a common empty response
			kvMsgSend = emptyResponse();

			switch (statusRecv) {
				case E2S_COMMAND_SERVER_RUN: {
					/* command server to run */
					if (serStatus != SerStatus.SHUTTING_DOWN) {
						/* if server is not shutting down, can change to running. */
						ptrKVServer.setSerStatus(SerStatus.RUNNING);
						if (!sendKVMessage(kvMsgSend, output)) { 	// empty response
							close();
							return;
						}
						continue;
					}
					/* server wants to shut down. */
					shutdownProcess();
					return;
				}
				case E2S_EMPTY_CHECK: {
					/* regular checks */
					if (serStatus == SerStatus.SHUTTING_DOWN) {
						/* server wants to shut down. */
						shutdownProcess();
						return;
					}
					/* regular response */
					sendKVMessage(kvMsgSend, output);
					continue;
				}
				case E2S_WRITE_LOCK_WITH_KEYRANGE: {
					/* write lock process */
					writeLockProcess(kvMsgRecv);
					continue;
				}
				case E2S_UPDATE_META_AND_RUN: {
					/* update metadata */
					if (ptrKVServer.setMetadata(valueRecv)) {
						ptrKVServer.setSerStatus(SerStatus.RUNNING);
					} else {
						close();
						return;
					}
					sendKVMessage(kvMsgSend, output);
					continue;
				}
				default: {
					logger.error("Invalid message <" + statusRecv.name()
								+ "> received at ECS child socket of server #"
								+ serverPort + " connected to IP: '"+ ecsIP
								+ "' \t port: " + ecsPort);
					close();
					return;
				}
			}
		}
	}

	private void shutdownProcess() {
		try {
			logger.info("Started shutdown communication between server #" + serverPort
						+ " and the ECS at IP: '" + ecsIP + "' \t port: " + ecsPort + "|ECS.");
			KVMessage kvMsgSend;
			KVMessage kvMsgRecv;
			// 1. request ECS for shutdown
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2E_SHUTDOWN_REQUEST);
			if (!sendKVMessage(kvMsgSend, output)) {
				ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
				close();
				return;
			}
			// 2. receive Write Lock with KeyRange
			kvMsgRecv = receiveKVMessage(input);
			if (kvMsgRecv.getStatus() == StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE) {
				;
			} else {
				// try receiving a the Write Lock again
				kvMsgSend = emptyResponse();
				if (!sendKVMessage(kvMsgSend, output)) {
					ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
					close();
					return;
				}
				kvMsgRecv = receiveKVMessage(input);
				if (kvMsgRecv.getStatus() == StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE) {
					;
				} else {
					logger.error("ECS did not respond to server #" + serverPort + "'s "
								+ "Shutdown request. Exiting.");
					close();
					return;
				}
			}
			// quick shutdown (for being the last server)
			String key = kvMsgRecv.getKey();
			if (Integer.parseInt(key) == serverPort) {
				logger.debug("This server #" + serverPort + " is the last server. "
							+ "Skipping move files and re-init contact. Closing right now...");
				ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
				close();
				return;
			}
			String value = kvMsgRecv.getValue();
			// 3. move KV pairs to new server.
			// "storeDir_predecessor,RangeFrom_this,RangeTo_this,IP_predecessor,L-port_predecessor"
			String[] valueSplit = value.split(",");
			String dirNewStore = valueSplit[0];
			BigInteger range_from = new BigInteger(valueSplit[1], 16);
			BigInteger range_to = new BigInteger(valueSplit[2], 16);
			List<BigInteger> range = new ArrayList<>();
			range.add(0, range_from);
			range.add(1, range_to);
			ptrKVServer.moveFilesTo(dirNewStore, range);
			// 4. notify the server, wait for it to re-initialize its store
			String tHostname = valueSplit[3];
			int tPort = Integer.parseInt(valueSplit[4]);
			logger.info("Message the KVServer at host: '" + tHostname + "' \t port: " + tPort
						+ " to reinitialize its Store since server #" + serverPort + " is shutting down.");
			Socket tSock = new Socket(tHostname, tPort);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2A_FINISHED_FILE_TRANSFER);
			kvMsgSend.setKey("" + serverPort); 		// use own server port to help the receiver to distinguish
			try {
				sendKVMessage(kvMsgSend, tOutput);
				receiveKVMessage(tInput); 			// wait for the target server to close the socket.
			} catch (Exception e) {
				// ignore
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
			}
			// 5. finally, notify ECS, wait for acknowledge, then terminate.
			try {
				sendKVMessage(kvMsgSend, output);
				receiveKVMessage(input); 			// wait for the ECS to close the socket.
			} catch (Exception e) {
				// ignore
			}
		} catch (Exception e) {
			logger.error("Exception during shutdown process in ECS child of server #"
						+ serverPort+ " connected to IP: '"+ ecsIP + "' \t port: "
						+ ecsPort + "|ECS.", e);
		} finally {
			ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
			close();
		}
	}

	void writeLockProcess(KVMessage kvMsgRecv_WL) {
		// last must be a SEND
		try {
			logger.info("Started Write Lock communication between server #" + serverPort
						+ " and the ECS at IP: '" + ecsIP + "' \t port: " + ecsPort + "|ECS.");
			KVMessage kvMsgSend;
			KVMessage kvMsgRecv;
			// 1. move KV pairs to new server.
			kvMsgRecv = kvMsgRecv_WL;
			String value = kvMsgRecv.getValue();
			// value: Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port
			String[] valueSplit = value.split(",");
			String dirNewStore = valueSplit[0];
			BigInteger range_from = new BigInteger(valueSplit[1], 16);
			BigInteger range_to = new BigInteger(valueSplit[2], 16);
			List<BigInteger> range = new ArrayList<>();
			range.add(0, range_from);
			range.add(1, range_to);
			ptrKVServer.moveFilesTo(dirNewStore, range);
			// 2. notify the server, wait for it to re-initialize its store
			String tHostname = valueSplit[3];
			int tPort = Integer.parseInt(valueSplit[4]);
			logger.info("Message the KVServer at host: '" + tHostname + "' \t port: " + tPort
						+ " to reinitialize its Store since server #" + serverPort + " sent files over.");
			Socket tSock = new Socket(tHostname, tPort);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2A_FINISHED_FILE_TRANSFER);
			kvMsgSend.setKey("" + serverPort); 		// use own server port to help the receiver to distinguish
			try {
				sendKVMessage(kvMsgSend, tOutput);
				receiveKVMessage(tInput); 			// wait for the target server to close the socket.
			} catch (Exception e) {
				// ignore
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
			}
			// 3. finally, notify ECS, and continue the cycle.
			kvMsgSend.setKey("" + tPort);
			sendKVMessage(kvMsgSend, output);
		} catch (Exception e) {
			logger.error("Exception during write lock process in ECS child of server #"
						+ serverPort+ " connected to IP: '"+ ecsIP + "' \t port: "
						+ ecsPort + "|ECS.", e);
			ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
			close();
		}
	}

	private KVMessage emptyResponse() {
		KVMessage kvMsg = new KVMessage();
		kvMsg.setStatus(StatusType.S2E_EMPTY_RESPONSE);
		return kvMsg;
	}

	/**
	 * Close ECS socket and change it to null. Also, prepare the shutdown of the server.
	 */
	private void close() {
		if (ecsSocket == null) {
			return;
		}
		if (ecsSocket.isClosed()) {
			ecsSocket = null;
			return;
		}
		try {
			logger.debug("ECS child of server #" + serverPort + " closing ECS socket"
						+ " connected to IP: '" + ecsIP + "' \t port: "
						+ ecsPort + ". This Thread is exiting very soon.");
			ecsSocket.close();
		} catch (Exception e) {
			logger.error("Unexpected Exception! Unable to close ECS child socket of "
						+ "server #" + serverPort + " connected to IP: '" + ecsIP
						+ "' \t port: " + ecsPort + ". This thread is exiting very soon.", e);
			// unsolvable error, thread must be shut down now
		} finally {
			ecsSocket = null;
			ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
		}
	}

	/**
	 * Universal method to SEND a KVMessage via socket; will log the message.
	 * Does NOT throw an exception.
	 * @param kvMsg a KVMessage object
	 * @return true for success, false otherwise
	 */
	private boolean sendKVMessage(KVMessage kvMsg, DataOutputStream output) {
		try {
			kvMsg.logMessageContent(false);
			byte[] bytes_msg = kvMsg.toBytes();
			// LV structure: length, value
			output.writeInt(bytes_msg.length);
			output.write(bytes_msg);
			if (kvMsg.getStatus() != StatusType.S2E_EMPTY_RESPONSE) {
				// do not log empty response
				logger.debug("#" + serverPort + "-" + ecsPort + "|ECS: "
							+ "Sending 4(length int) " +  bytes_msg.length + " bytes.");
			}
			output.flush();
		} catch (Exception e) {
			logger.error("Exception when server #" + serverPort + " sends to IP: '"
						+ ecsIP + "' \t port: " + ecsPort
						+ "|ECS. Should close socket to unblock ECS's receive().", e);
			return false;
		}
		return true;
	}

	/**
	 * Universal method to RECEIVE a KVMessage via socket; will log the message.
	 * @return a KVMessage object
	 * @throws Exception throws exception as closing socket causes receive() to unblock.
	 */
	private KVMessage receiveKVMessage(DataInputStream input) throws Exception {
		KVMessage kvMsg = new KVMessage();
		// LV structure: length, value
		int size_bytes = input.readInt();
		byte[] bytes = new byte[size_bytes];
		input.readFully(bytes);
		if (!kvMsg.fromBytes(bytes)) {
			throw new Exception("#" + serverPort + "-" + ecsPort + "|ECS: "
								+ "Cannot convert all received bytes to KVMessage.");
		}
		if (kvMsg.getStatus() != StatusType.E2S_EMPTY_CHECK) {
			// do not log empty check
			logger.debug("#" + serverPort + "-" + ecsPort + "|ECS: "
						+ "Receiving 4(length int) + " + size_bytes + " bytes.");
		}
		kvMsg.logMessageContent(true);
		return kvMsg;
	}
}
