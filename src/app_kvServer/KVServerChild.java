package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;
import app_kvServer.IKVServer.SerStatus;
import app_kvServer.KVServer.Replica;

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
				kvMsgRecv = receiveKVMessage(input);
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
					if (!sendKVMessage(kvMsgSend, output)) {
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
				if (!sendKVMessage(kvMsgSend, output)) {
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
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
				KVMessage kvMsgSend = new KVMessage();
				kvMsgSend.setValue(ptrKVServer.getMetadataRead());
				/*
				 * Client commands 'keyrange', server is not stopped or shutting down,
				 * reply with 'keyrange success' and latest server metadata
				 */
				kvMsgSend.setStatus(StatusType.KEYRANGE_READ_SUCCESS);
				if (!sendKVMessage(kvMsgSend, output)) {
					close();
					return;
				}
				continue;
			} else if (statusRecv == StatusType.S2A_FINISHED_FILE_TRANSFER) {
				/* SPECIAL CASE: this server just received KV pairs in its disk storage. */
				if (Integer.parseInt(keyRecv) == serverPort) {
					// sender is youself, can happen when you are the last node shutting down
					logger.error("IMPOSSIBLE TO CONTACT YOURSELF.");
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
					Thread.sleep(200);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				if (!ptrKVServer.reInitializeCoordinatorStore()) {
					/* Fatal error, the shared Store cannot be re-initialized. */
					ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
				} else {
					ptrKVServer.setSerStatus(serStatus_Copy);
				}
				// one-time connection.
				try {
					sendKVMessage(emptyResponse(), output);
					receiveKVMessage(input);
				} catch (Exception e) {
					;
				} finally {
					logger.debug("FINISHED REINIT STORE.");
					close();
					return;
				}
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
					if (!sendKVMessage(kvMsgSend, output)) {
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
					if (!sendKVMessage(kvMsgSend, output)) {
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
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			}

			// 2. server not responsible
			if (statusRecv == StatusType.PUT) {
				if (!ptrKVServer.isCoordinatorForKey(keyRecv)) {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setValue(ptrKVServer.getMetadata());
					/*
					 * This server is not Coordinator for PUT this key,
					 * reply with latest metadata.
					 */
					kvMsgSend.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			} else if (statusRecv == StatusType.GET) {
				if (!ptrKVServer.isReplicaForKey(keyRecv)) {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setValue(ptrKVServer.getMetadataRead());
					/*
					 * This server is not Replica for GET this key,
					 * reply with latest metadata_read.
					 */
					kvMsgSend.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			}

			// 3. server-to-server PUT propagation
			if ((statusRecv == StatusType.S2S_SERVER_PUT)
				&& (serStatus != SerStatus.SHUTTING_DOWN)) {
				/*
				 * Server-to-Server PUT Propagation.
				 * Find the corresponding Replica store, or re-init All Replicas' store
				 */
				BigInteger hashedKey = hash(keyRecv);
				try {
					ConcurrentHashMap<Integer,Replica> replicas = ptrKVServer.getReplicas();
					boolean found = false;
					for (Map.Entry<Integer, Replica> entry : replicas.entrySet()) {
						if (isBounded(hashedKey,
									  entry.getValue().rangeFrom_Replica,
									  entry.getValue().rangeTo_Replica)) {
							Store repStore = entry.getValue().store;
							if (valueRecv == null) {
								/* DELETE */
								if (repStore.containsKey(keyRecv)) {
									repStore.delete(keyRecv);
									logger.debug("[To Client] >>> Propagate-deleted '" + keyRecv + "'");
								}
							} else if (!repStore.containsKey(keyRecv)) {
								/* PUT */
								repStore.put(keyRecv, valueRecv);
								logger.debug("[To Client] >>> Propagate-put '" + keyRecv + "'");
							} else {
								/* UPDATE */
								repStore.update(keyRecv, valueRecv);
								logger.debug("[To Client] >>> Propagate-update '" + keyRecv + "'");
							}
							found = found || true;
						}
					}
					if (!found) {
						replicas = null;
						logger.debug("[To Client] >>> Cannot find Replica for PUT propagation, "
									+ "Re-initializing Replicas store...");
						SerStatus serStatus_Copy = serStatus;
						ptrKVServer.setSerStatus(SerStatus.STOPPED);
						ptrKVServer.reInitializeAllReplicasStore();
						ptrKVServer.setSerStatus(serStatus);
						logger.debug("[To Client] >>> Replica re-initialization done. Retrying.");
					} else {
						close();
						return;
					}
					replicas = ptrKVServer.getReplicas();
					found = false;
					for (Map.Entry<Integer, Replica> entry : replicas.entrySet()) {
						if (isBounded(hashedKey,
									  entry.getValue().rangeFrom_Replica,
									  entry.getValue().rangeTo_Replica)) {
							Store repStore = entry.getValue().store;
							if (valueRecv == null) {
								/* DELETE */
								if (repStore.containsKey(keyRecv)) {
									repStore.delete(keyRecv);
									logger.debug("[To Client] >>> Propagate-deleted '" + keyRecv + "'");
								}
							} else if (!repStore.containsKey(keyRecv)) {
								/* PUT */
								repStore.put(keyRecv, valueRecv);
								logger.debug("[To Client] >>> Propagate-put '" + keyRecv + "'");
							} else {
								/* UPDATE */
								repStore.update(keyRecv, valueRecv);
								logger.debug("[To Client] >>> Propagate-update '" + keyRecv + "'");
							}
							found = found || true;
						}
					}
					if (!found) {
						logger.error("[To Client] >>> Cannot find corresponding Replica even after "
									+ "re-initialization for PUT propagation.");
					}
				} catch (Exception e) {
					logger.error("[To Client] >>> Exception during receiving PUT propagation.", e);
				} finally {
					close();
					return;
				}
			}
			
			// 4. normal KV response to client
			switch (statusRecv) {
				case GET: {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (ptrKVServer.isCoordinatorForKey(keyRecv)) {
						/* found in Coordinator */
						if (!ptrKVServer.inStorage(keyRecv)) {
							/*
							* Key does not exist in storage,
							* reply GET with 'get error' and Key
							*/
							kvMsgSend.setStatus(StatusType.GET_ERROR);
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
								close();
								return;
							}
							continue;
						}
					} else {
						/* need to search in replicas */
						BigInteger hashedKey = hash(keyRecv);
						try {
							ConcurrentHashMap<Integer,Replica> replicas = ptrKVServer.getReplicas();
							boolean found = false;
							String value = null;
							for (Map.Entry<Integer, Replica> entry : replicas.entrySet()) {
								if (isBounded(hashedKey,
											entry.getValue().rangeFrom_Replica,
											entry.getValue().rangeTo_Replica)) {
									Store repStore = entry.getValue().store;
									String valueTmp = repStore.get(keyRecv);
									if (value == null) {
										found = found || false;
										continue;
									}
									found = found || true;
									value = valueTmp;
									continue;
								}
							}
							if (!found) {
								replicas = null;
								logger.debug("[To Client] >>> Cannot find Replica for GET, "
											+ "Re-initializing Replicas store...");
								SerStatus serStatus_Copy = serStatus;
								ptrKVServer.setSerStatus(SerStatus.STOPPED);
								ptrKVServer.reInitializeAllReplicasStore();
								ptrKVServer.setSerStatus(serStatus);
								logger.debug("[To Client] >>> Replica re-initialization done. Retrying.");
								replicas = ptrKVServer.getReplicas();
								found = false;
								for (Map.Entry<Integer, Replica> entry : replicas.entrySet()) {
									if (isBounded(hashedKey,
												entry.getValue().rangeFrom_Replica,
												entry.getValue().rangeTo_Replica)) {
										Store repStore = entry.getValue().store;
										String valueTmp = repStore.get(keyRecv);
										if (value == null) {
											found = found || false;
											continue;
										}
										found = found || true;
										value = valueTmp;
										continue;
									}
								}
							}
							kvMsgSend.setValue(value);
							if (value == null) {
								kvMsgSend.setStatus(StatusType.GET_ERROR);
							} else {
								kvMsgSend.setStatus(StatusType.GET_SUCCESS);
							}
							if (!sendKVMessage(kvMsgSend, output)) {
								close();
								return;
							}
							continue;
						} catch (Exception e) {
							logger.error("[To Client] >>> Exception during GET in Replicas.", e);
							kvMsgSend = new KVMessage();
							kvMsgSend.setStatus(StatusType.GET_ERROR);
							kvMsgSend.setKey(keyRecv);
							if (!sendKVMessage(kvMsgSend, output)) {
								close();
								return;
							}
						}
					}
				}
				case PUT: {
					// propagate client's PUT request to servers with this Coordinator's Replicas
					if (!propagatePut(keyRecv, valueRecv)) {
						logger.error("[To Client] >>> PUT propagation unsuccessful. Continue on.");
					}
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
							if (!sendKVMessage(kvMsgSend, output)) {
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
						if (!sendKVMessage(kvMsgSend, output)) {
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
						if (!sendKVMessage(kvMsgSend, output)) {
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
	 * Server's put to update replicas of this coordinator in other server
	 * @param key client's PUT key
	 * @param value client's PUT value
	 * @return true for success, false otherwise
	 */
	private boolean propagatePut(String key, String value) {
		try {
			boolean isSuccess = true;
			String meta = ptrKVServer.getMetadata();
			int portSuccessor = findSuccessorPort(meta, serverPort);
			if (portSuccessor == serverPort) {
				// Case: 1 server
				return isSuccess;
			}
			// Cases: 2+ servers
			logger.debug("[To Client] >>> propagate PUT to #" + portSuccessor);
			isSuccess = isSuccess && connectAndServerPut(portSuccessor, key, value);
			int portSSuccessor = findSuccessorPort(meta, portSuccessor);
			if (portSSuccessor == serverPort) {
				// Case: 2 servers
				return isSuccess;
			}
			// Cases: 3+ servers
			logger.debug("[To Client] >>> propagate PUTto #" + portSSuccessor);
			isSuccess = isSuccess && connectAndServerPut(portSSuccessor, key, value);
			return isSuccess;
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during PUT propagation. ", e);
			return false;
		}
	}

	/**
	 * Connect to the listening socket of specified KVServer and send SERVER PUT
	 * @param port specified KVServer's port
	 * @param key client's PUT key
	 * @param value client's PUT value
	 * @return
	 */
	private boolean connectAndServerPut(int port, String key, String value) {
		try {
			Socket tSock = new Socket(responseIP, port);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			KVMessage kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2S_SERVER_PUT);
			kvMsgSend.setKey(key);
			kvMsgSend.setValue(value);
			if (!sendKVMessage(kvMsgSend, tOutput)) {
				logger.error("[To Client] >>> Cannot contact #" + port + " to propagate PUT.");
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
				return false;
			}
			try {
				// expect target server to close the connection
				receiveKVMessage(tInput);
			} catch (Exception e) {
				logger.debug("[To Client] >>> Finish PUT propagation to server #" + port + ".");
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
				return true;
			}
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during connection for propagating PUT.", e);
		}
		return false;
	}

	/**
	 * Return the port of the succesor of the server with a specific port.
	 * @param meta metadata
	 * @param portCurrent port of current server (not necessarily this server)
	 * @return port of successor server of the specified
	 */
	private int findSuccessorPort(String meta, int portCurrent) throws Exception{
		String[] entries = meta.split(";");
		String[] elements = entries[0].split(",");
		if (elements.length != 4) {
			throw new Exception("Exception! metadata format cannot be recognized!");
		}
		String rangeTo_Current_Str = "";
		for (String entry : entries) {
			// range_from,range_to,ip,port
			elements = entry.split(",");
			if (Integer.parseInt(elements[3]) != portCurrent) {
				continue;
			}
			rangeTo_Current_Str = elements[1];
			break;
		}
		if (rangeTo_Current_Str.equals("")) {
			throw new Exception("Exception! Cannot find rangeTo of server #" + portCurrent);
		}
		BigInteger rangeTo_Current = new BigInteger(rangeTo_Current_Str, 16);
		BigInteger rangeFrom_Successor;
		if (rangeTo_Current.equals(new BigInteger("0"))) {
			rangeFrom_Successor = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
		} else {
			rangeFrom_Successor = rangeTo_Current.subtract(new BigInteger("1"));
		}
		String rangeFrom_Successor_Str = rangeFrom_Successor.toString(16);
		for (String entry : entries) {
			// range_from,range_to,ip,port
			elements = entry.split(",");
			if (elements[0].equals(rangeFrom_Successor_Str)) {
				return Integer.parseInt(elements[3]);
			}
		}
		throw new Exception("Exception! Cannot find rangeFrom '" + rangeFrom_Successor_Str
							+ "' from metadata.");
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
	private boolean sendKVMessage(KVMessage kvMsg, DataOutputStream output) {
		try {
			kvMsg.logMessageContent(false);
			byte[] bytes_msg = kvMsg.toBytes();
			// LV structure: length, value
			output.writeInt(bytes_msg.length);
			output.write(bytes_msg);
			output.flush();
		} catch (Exception e) {
			logger.error("[To Client] >>> Unexpected exception in sendKVMessage().", e);
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

	private KVMessage emptyResponse() {
		KVMessage kvMsg = new KVMessage();
		kvMsg.setStatus(StatusType.S2E_EMPTY_RESPONSE);
		return kvMsg;
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
}
