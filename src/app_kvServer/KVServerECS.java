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
			logger.error("[To ECS] >>> Failed to create socket data stream. Terminating server.", e);
			close();
			return;
		}

		SerStatus serStatus;
		KVMessage kvMsgSend;
		KVMessage kvMsgRecv;
		StatusType statusRecv;
		String keyRecv;
		String valueRecv;

		// initialization: init request with dir
		logger.debug("[To ECS] >>> Starting initialization communication...");
		kvMsgSend = new KVMessage();
		kvMsgSend.setStatus(StatusType.S2E_INIT_REQUEST_WITH_DIR);
		kvMsgSend.setKey(Integer.toString(serverPort)); 	// server's listening port, for moving files
		kvMsgSend.setValue(ptrKVServer.getDirStore()); 		// server's store directory
		if (!sendKVMessage(kvMsgSend, output)) {
			logger.error("[To ECS] >>> Failed to send INIT REQUEST. Terminating server.");
			close();
			return;
		}
		// expect: init response with meta
		try {
			kvMsgRecv = receiveKVMessage(input);
		} catch (Exception e) {
			logger.error("[To ECS] >>> Lost connection during initialization. Terminating server.", e);
			close();
			return;
		}
		statusRecv = kvMsgRecv.getStatus();
		if (statusRecv == StatusType.E2S_INIT_RESPONSE_WITH_META) {
			valueRecv = kvMsgRecv.getValue(); 	// keyrange metadata
			if (!ptrKVServer.setMetadata(valueRecv)) {
				logger.error("[To ECS] >>> Failed to set metedata with '" + valueRecv 
							+ "'. Terminating server.");
				close();
				return;
			}
		} else {
			// unexpected message
			logger.error("[To ECS] >>> Got wrong response for INIT REQUEST, which is " + statusRecv.name()
						+ ". Terminating server.");
			close();
			return;
		}
		logger.debug("[To ECS] >>> Received INIT response, got initial metadata. Begin main loop.");


		loop: while (true) {
			// receive message from ECS. (ECS sends regular check to server.)
			try {
				kvMsgRecv = receiveKVMessage(input);
			} catch (Exception e) {
				logger.error("[To ECS] >>> Connection lost when receiving ECS message. Terminating server.");
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
						if (!sendKVMessage(kvMsgSend, output)) { 	// reply with an empty response
							logger.error("[To ECS] >>> Failed to reply to COMMAND RUN. Terminating server.");
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
					/* regular empty checks */
					if (serStatus == SerStatus.SHUTTING_DOWN) {
						/* server wants to shut down. */
						shutdownProcess();
						return;
					}
					/* regular empty response */
					if (!sendKVMessage(kvMsgSend, output)) {
						logger.error("[To ECS] >>> Failed to reply to EMPTY CHECK. Terminating server.");
						close();
						return;
					}
					continue;
				}
				case E2S_WRITE_LOCK_WITH_KEYRANGE: {
					/* write lock process */
					if (!writeLockProcess(kvMsgRecv)) {
						return;
					}
					continue;
				}
				case E2S_UPDATE_META_AND_RUN: {
					/* update metadata */
					if (ptrKVServer.setMetadata(valueRecv)) {
						ptrKVServer.setSerStatus(SerStatus.RUNNING);
					} else {
						logger.error("[To ECS] >>> Failed to update metadata. Terminating server.");
						close();
						return;
					}
					if (!sendKVMessage(kvMsgSend, output)) {
						logger.error("[To ECS] >>> Failed to reply to UPDATE META. Terminating server.");
						close();
						return;
					}
					continue;
				}
				default: {
					logger.error("[To ECS] >>> Received unexpected message '" + statusRecv.name()
								+ "'. Terminating server.");
					close();
					return;
				}
			}
		}
	}

	private void shutdownProcess() {
		try {
			KVMessage kvMsgSend;
			KVMessage kvMsgRecv;
			// 1. request ECS for shutdown
			logger.debug("[To ECS] >>> Requesting shutdown...");
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2E_SHUTDOWN_REQUEST);
			if (!sendKVMessage(kvMsgSend, output)) {
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
					logger.error("[To ECS] >>> Failed to receive shutdown WRITE LOCK. Terminating server.");
					close();
					return;
				}
				kvMsgRecv = receiveKVMessage(input);
				if (kvMsgRecv.getStatus() == StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE) {
					;
				} else {
					logger.error("[To ECS] >>> Did not receive shutdown WRITE LOCK. Terminating server.");
					close();
					return;
				}
			}
			// Special Case: last server is shutting down
			logger.debug("[To ECS] >>> Received the shutdown WRITE LOCK.");
			String key = kvMsgRecv.getKey();
			if ((key != null) && (Integer.parseInt(key) == serverPort)) {
				logger.debug("[To ECS] >>> I am the last server, shutting down. Exiting without "
							+ "the need for file transfer.");
				close();
				return;
			}
			// General Case: need to transfer files
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
			logger.debug("[To ECS] >>> Moving KV pairs to directory '" + dirNewStore + "'...");
			ptrKVServer.moveFilesTo(dirNewStore, range);

			// 4. notify the server, wait for it to re-initialize its store
			String tHostname = valueSplit[3];
			int tPort = Integer.parseInt(valueSplit[4]);
			logger.debug("[To ECS] >>> Finished moving KV pairs, contacting predecessor #" + tPort
						+ " to reinitialize Store.");
			Socket tSock = new Socket(tHostname, tPort);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2A_FINISHED_FILE_TRANSFER);
			kvMsgSend.setKey("" + serverPort); 		// provide its listening port to the predecessor
			try {
				if (!sendKVMessage(kvMsgSend, tOutput)) {
					logger.error("[To ECS] >>> Cannot contact predecessor server. Terminating server.");
					close();
					return;
				}
				receiveKVMessage(tInput); 			// expect predecessor to reply before closing
			} catch (Exception e) {
				logger.error("[To ECS] >>> Exception when communicating with predecessor. Terminating server.", e);
				close();
				return;
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
			}
			// 5. finally, notify ECS, wait for acknowledge, then terminate.
			try {
				if (!sendKVMessage(kvMsgSend, output)) {
					logger.error("[To ECS] >>> Cannot contact ECS for the completion of file transfer. Terminating server.");
					close();
					return;
				}
				receiveKVMessage(input); 			// wait for the ECS to close the socket.
			} catch (Exception e) {
				logger.debug("[To ECS] >>> Completed full shutdown process. Exiting this Thread.");
				// ignore
			}
		} catch (Exception e) {
			logger.error("[To ECS] >>> Exception during shutdown process. Terminating Thread.", e);
		} finally {
			close();
		}
	}

	boolean writeLockProcess(KVMessage kvMsgRecv_WL) {
		// last must be a SEND
		try {
			KVMessage kvMsgSend;
			KVMessage kvMsgRecv;
			kvMsgRecv = kvMsgRecv_WL;

			// 1. move KV pairs to new server.
			String value = kvMsgRecv.getValue();
			// value: Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port
			String[] valueSplit = value.split(",");
			String dirNewStore = valueSplit[0];
			BigInteger range_from = new BigInteger(valueSplit[1], 16);
			BigInteger range_to = new BigInteger(valueSplit[2], 16);
			List<BigInteger> range = new ArrayList<>();
			range.add(0, range_from);
			range.add(1, range_to);
			logger.debug("[To ECS] >>> Receiving WRITE LOCK, moving KV pairs to directory '"
						+ dirNewStore + "'...");
			ptrKVServer.moveFilesTo(dirNewStore, range);
			// 2. notify the server, wait for it to re-initialize its store
			String tHostname = valueSplit[3];
			int tPort = Integer.parseInt(valueSplit[4]);
			logger.debug("[To ECS] >>> Finished moving KV pairs, contacting server #" + tPort
						+ " to reinitialize Store.");
			Socket tSock = new Socket(tHostname, tPort);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2A_FINISHED_FILE_TRANSFER);
			kvMsgSend.setKey("" + serverPort); 		// provide its listening port to the predecessor
			try {
				if (!sendKVMessage(kvMsgSend, tOutput)) {
					logger.error("[To ECS] >>> Cannot contact #" + tPort + ". Terminating server.");
					close();
					return false;
				}
				receiveKVMessage(tInput); 			// expect predecessor to reply before closing
			} catch (Exception e) {
				logger.error("[To ECS] >>> Exception when communicating with #" + tPort + ". Terminating server.", e);
				close();
				return false;
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
			}
			// 3. finally, notify ECS, and continue the cycle.
			kvMsgSend.setKey("" + tPort);   // for ECS, send the port number of the server who just re-init its store
			logger.debug("[To ECS] >>> Contacted server #" + tPort + ", then contact ECS for the completion of file transfer.");
			if (!sendKVMessage(kvMsgSend, output)) {
				logger.error("[To ECS] >>> Cannot contact ECS for the completion of file transfer. Terminating server.");
				close();
				return false;
			}
		} catch (Exception e) {
			logger.error("[To ECS] >>> Exception during Write Lock process. Terminating thread.", e);
			close();
			return false;
		}
		logger.debug("[To ECS] >>> Completed full Write Lock process. Continue the main loop.");
		return true;
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
			logger.debug("[To ECS] >>> Closing socket for Server-to-ECS communication.");
			ecsSocket.close();
		} catch (Exception e) {
			logger.error("[To ECS] >>> Expection when closing Server-to-ECS socket. Still proceed.", e);
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
			output.flush();
		} catch (Exception e) {
			logger.error("[To ECS] >>> Unexpected exception in sendKVMessage().", e);
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
			throw new Exception("Exception! Cannot convert all bytes to KVMessage!");
		}
		kvMsg.logMessageContent(true);
		return kvMsg;
	}
}
