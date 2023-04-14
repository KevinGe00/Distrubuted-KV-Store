package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.Socket;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
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
	
	/* M4 */
	private ConcurrentHashMap<String, ConcurrentHashMap<String, String>> convertStringToTable(String table_string) {
		logger.debug(">>> string->table");
		logger.debug(table_string);
		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = new ConcurrentHashMap<>();
		if ((table_string == null) || (table_string.equals(""))) {
			return table;
		}

		String doubleLineSep = System.lineSeparator() + System.lineSeparator();
		String singleLineSep = System.lineSeparator();
		String[] rows_cols_values = table_string.split(doubleLineSep, 3);
        String[] rows = rows_cols_values[0].split(singleLineSep);
		logger.debug("row names:" + rows.length);
        String[] cols = rows_cols_values[1].split(singleLineSep);
		logger.debug("column names:" + cols.length);
        String[] values = rows_cols_values[2].split(singleLineSep);
		logger.debug("values:" + values.length);

        int num_row = rows.length;
        int num_col = cols.length;

		// rows
		for (int idx_row = 0; idx_row < num_row; idx_row++) {
			String row_name = rows[idx_row];
			ConcurrentHashMap<String, String> subtable_row = new ConcurrentHashMap<>();
			// values (in each column)
			for (int idx_col = 0; idx_col < num_col; idx_col++) {
				String col_name = cols[idx_col];
				int idx_value = idx_row * num_col + idx_col;
				String cell_value = values[idx_value];
				subtable_row.put(col_name, cell_value);
			}
			table.put(row_name, subtable_row);
		}
		logger.debug(table);
		return table;
	}

	/* M4 */
	private String convertTableToString(ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table) {
		logger.debug(">>> table->string");
		logger.debug(table);
		StringBuilder table_string = new StringBuilder();

		String doubleLineSep = System.lineSeparator() + System.lineSeparator();
		String singleLineSep = System.lineSeparator();

		// special case: empty table, to be delete
		if (table.size() == 0) {
			return "";
		}
		// non-empty table

		// get all row names in order
		TreeSet<String> row_names = new TreeSet<String>();
		row_names.addAll(table.keySet());
		
		// get all column names in order
		TreeSet<String> col_names = new TreeSet<String>();
		for (String row_name : row_names) {
			ConcurrentHashMap<String, String> subtable_row = table.get(row_name);
			Set<String> this_col_names = subtable_row.keySet();
			col_names.addAll(this_col_names);
		}
		
		// print row names
		for (String row_name : row_names) {
			table_string.append(row_name + singleLineSep);
		}
		table_string.append(singleLineSep);
		
		// print column names
		for (String col_name : col_names) {
			table_string.append(col_name + singleLineSep);
		}

		// print value in rows
		for (String row_name : row_names) {
			ConcurrentHashMap<String, String> subtable_row = table.get(row_name); // always exists
			for (String col_name : col_names) {
				String cell_value = subtable_row.get(col_name);
				if (cell_value == null) {
					cell_value = " ";
				}
				table_string.append(singleLineSep);
				table_string.append(cell_value);
			}
		}
		logger.debug(table_string.toString());
		return table_string.toString();
	}

	/* M4 */
	private ConcurrentHashMap<String, ConcurrentHashMap<String, String>> solveTablePut(
		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table,
		String valueRecv) {
		logger.debug(">>> TABLE_PUT");
		String singleLineSep = System.lineSeparator();

		String[] row_col_value = valueRecv.split(singleLineSep);
		String row_name = row_col_value[0];
		String col_name = row_col_value[1];
		String cell_value = row_col_value[2];

		// check if row exists, if not, create new row; if so, get the existing row
		ConcurrentHashMap<String, String> subtable_row = table.get(row_name);
		if (subtable_row == null) {
			subtable_row = new ConcurrentHashMap<>();
		}
		// put/update the cell with the new value
		subtable_row.put(col_name, cell_value);
		table.put(row_name, subtable_row);
		
		logger.debug(table);
		return table;
	}

	/* M4, note: does not return the deleted value */
	private ConcurrentHashMap<String, ConcurrentHashMap<String, String>> solveTableDelete(
		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table,
		String valueRecv) {
		logger.debug(">>> TABLE_DELETE");
		String singleLineSep = System.lineSeparator();

		String[] row_col = valueRecv.split(singleLineSep);
		String row_name = row_col[0];
		String col_name = row_col[1];

		// check if row exists, if not, done; if so, get the existing row
		ConcurrentHashMap<String, String> subtable_row = table.get(row_name);
		if (subtable_row == null) {
			logger.debug(table);
			return table;
		}
		// delete the key
		subtable_row.remove(col_name);
		// check if the new row is empty, if not, put it back to table; if s0, delete the row as well
		if (subtable_row.size() > 0) {
			table.put(row_name, subtable_row);
		} else {
			table.remove(row_name);
		}
		logger.debug(table);
		return table;
	}

	/* M4, note: return the same valueRecv if failed; return extended if succeeded */
	private String solveTableCell (ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table, String valueRecv) {
		logger.debug(">>> TABLE_GET");
		logger.debug(table);
		String singleLineSep = System.lineSeparator();

		String[] row_col = valueRecv.split(singleLineSep);
		String row_name = row_col[0];
		String col_name = row_col[1];

		// check if row exists, if not, done; if so, get the existing row
		ConcurrentHashMap<String, String> subtable_row = table.get(row_name);
		if (subtable_row == null) {
			return valueRecv;
		}
		// get the cell by the column name
		String cell_value = subtable_row.get(col_name);
		// check if the cell is NULL or empty, if not, return extended value; if so, done
		if ((cell_value != null) && !cell_value.isEmpty()) {
			String valueRecv_extend = valueRecv + singleLineSep + cell_value;
			return valueRecv_extend;
		} else {
			return valueRecv;
		}
	}

	/* M4 */
	private String solveTableSelect (ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table, String valueRecv) {
		logger.debug(">>> TABLE_SELECT");
		logger.debug(table);
		if (table.size() == 0) {
			return valueRecv;
		}
		
		String singleLineSep = System.lineSeparator();

		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table_select = new ConcurrentHashMap<>();

		ArrayList<String> blacklist = new ArrayList<>();

		String[] conditions = valueRecv.split(singleLineSep);
		// for each condition, seperate it by the '>', '<'
		for (String condition : conditions) {
			String key = "";
			String symbol = "";
			int threshold = 0;
			if (condition.contains(">")) {
				String[] key_threshold = condition.split(">");
				key = key_threshold[0];
				symbol = ">";
				threshold = Integer.parseInt(key_threshold[1]);
			} else if (condition.contains("<")) {
				String[] key_threshold = condition.split("<");
				key = key_threshold[0];
				symbol = "<";
				threshold = Integer.parseInt(key_threshold[1]);
			} else {
				key = condition;
			}
			
			// loop through all rows
			for (String row_name : table.keySet()) {	
				if (blacklist.contains(row_name)) {
					continue;
				}
				ConcurrentHashMap<String, String> subtable_row = table.get(row_name); // must exists
				// get selected column from this row
				String cell_value = subtable_row.get(key).replace("\r", "").replace("\n", "");
				// if entry is empty, skip
				if ((cell_value == null) || (cell_value.equals(" "))) {
					continue;
				}
				// if not, check if satisfies the condition
				if (symbol.equals(">")) {
					int num_cellValue = Integer.parseInt(cell_value);
					if (!(num_cellValue > threshold)) {
						blacklist.add(row_name);
						ConcurrentHashMap<String, String> row_exist = table_select.get(row_name);
						if (row_exist == null) {
							continue;
						}
						table_select.remove(row_name);
						continue;
					}
				} else if (symbol.equals("<")) {
					int num_cellValue = Integer.parseInt(cell_value);
					if (!(num_cellValue < threshold)) {
						blacklist.add(row_name);
						ConcurrentHashMap<String, String> row_exist = table_select.get(row_name);
						if (row_exist == null) {
							continue;
						}
						table_select.remove(row_name);
						continue;
					}
				}
				// condition satisfied, collect this cell
				// if this row already exists in table_select, get it from table_select
				ConcurrentHashMap<String, String> row_new = table_select.get(row_name);
				if (row_new == null) {
					row_new = new ConcurrentHashMap<>();
				}
				// put the cell in the new table row
				row_new.put(key, cell_value);
				// put the new row back
				table_select.put(row_name, row_new);
			}
		}

		return convertTableToString(table_select);
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
					Thread.sleep(50);
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
			if ((statusRecv == StatusType.GET)
				/* M4 */
				|| (statusRecv == StatusType.TABLE_GET) || (statusRecv == StatusType.TABLE_SELECT)) {
				if ((serStatus == SerStatus.STOPPED)
					|| (serStatus == SerStatus.SHUTTING_DOWN)) {
					/*
					 * Server is stopped or shutting down,
					 * reply GET with 'server stopped' and full GET message.
					 */
					// M4, TABLE_GET and TABLE_SELECT share server-response logic
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setStatus(StatusType.SERVER_STOPPED);
					kvMsgSend.setKey(keyRecv);
					kvMsgSend.setValue(valueRecv);
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			} else if ((statusRecv == StatusType.PUT)
						/* M4 */
						|| (statusRecv == StatusType.TABLE_PUT) || (statusRecv == StatusType.TABLE_DELETE)) {
				if ((serStatus == SerStatus.STOPPED)
					|| (serStatus == SerStatus.SHUTTING_DOWN)) {
					/*
					 * Server is stopped or shutting down,
					 * reply PUT with 'server stopped' and full PUT message.
					 */
					// M4, TABLE_PUT and TABLE_DELETE share server-response logic (stopped)
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
					// M4, TABLE_PUT and TABLE_DELETE share server-response logic (write lock)
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
			if ((statusRecv == StatusType.PUT)
				/* M4 */
				|| (statusRecv == StatusType.TABLE_PUT) || (statusRecv == StatusType.TABLE_DELETE)) {
				if (!ptrKVServer.isCoordinatorForKey(keyRecv)) {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setValue(ptrKVServer.getMetadata());
					/*
					 * This server is not Coordinator for PUT this key,
					 * reply with latest metadata.
					 */
					// M4, TABLE_PUT and TABLE_DELETE share not-responsible logic
					kvMsgSend.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			} else if ((statusRecv == StatusType.GET)
						/* M4 */
						|| (statusRecv == StatusType.TABLE_GET) || (statusRecv == StatusType.TABLE_SELECT)) {
				if (!ptrKVServer.isReplicaForKey(keyRecv)) {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setValue(ptrKVServer.getMetadataRead());
					/*
					 * This server is not Replica for GET this key,
					 * reply with latest metadata_read.
					 */
					// M4, TABLE_GET and TABLE_SELECT share not-responsible
					kvMsgSend.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
					if (!sendKVMessage(kvMsgSend, output)) {
						close();
						return;
					}
					continue;
				}
			}

			// 3. server-to-server PUT propagation (only put into replicas, impossible to put to coordinator)
			/* M4 addition */
			if (((statusRecv == StatusType.S2S_SERVER_PUT)
				 || (statusRecv == StatusType.S2S_SERVER_TABLE_PUT)
				 || (statusRecv == StatusType.S2S_SERVER_TABLE_DELETE))
				&& (serStatus != SerStatus.SHUTTING_DOWN)) {
				/*
				 * Server-to-Server PUT Propagation.
				 * Find the corresponding Replica store, or re-init All Replicas' store
				 */
				// M4, S2S TABLE PUT and S2S TABLE DELETE propagation use the same logic
				BigInteger hashedKey = hash(keyRecv);
				try {
					ConcurrentHashMap<Integer,Replica> replicas = ptrKVServer.getReplicas();
					boolean found = false;
					Replica thisReplica;
					Iterator<Map.Entry<Integer,Replica>> iter = replicas.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry<Integer,Replica> entry = iter.next();
						thisReplica = entry.getValue();
						/* first, check if any replica is outdated, if so delete them */
						if ((!ptrKVServer.getMetadata().contains(thisReplica.port+""))
							|| (isBounded(thisReplica.rangeFrom_Replica,
									      ptrKVServer.rangeFrom_Coordinator,
									      ptrKVServer.rangeTo_Coordinator))
							|| (!isBounded(thisReplica.rangeFrom_Replica,
										   ptrKVServer.rangeFrom_AllReplicas,
										   ptrKVServer.rangeTo_AllReplicas))) {
							// this Replica has been outdated, remove this
							logger.debug("[To Client] >>> Deleting an outdated Replica #"
										+ thisReplica.port + "...");
							String folderRemove = thisReplica.dirStore;
							File foldRemove = new File(folderRemove);
							if (foldRemove.exists()) {
								try {
									File[] files = foldRemove.listFiles();
									for (File file : files) {
										file.delete();
									}
									foldRemove.delete();
								} catch (Exception e) {
									logger.error("[To Client] >>> Exception when deleting outdated store files.", e);
								}
							}
							iter.remove();
							continue;
						}

						if (isBounded(hashedKey,
									  thisReplica.rangeFrom_Replica,
									  thisReplica.rangeTo_Replica)) {
							Store repStore = thisReplica.store;
							/* M4 addition */
							if (statusRecv == StatusType.S2S_SERVER_PUT) {
								if (valueRecv == null) {
									/* DELETE */
									if (repStore.containsKey(keyRecv)) {
										repStore.delete(keyRecv);
										logger.debug("[To Client] >>> Propagate-deleted '" + keyRecv + "'");
										found = found || true;
									} else {
										found = found || false;
									}
								} else if (!repStore.containsKey(keyRecv)) {
									/* PUT */
									repStore.put(keyRecv, valueRecv);
									logger.debug("[To Client] >>> Propagate-put '" + keyRecv + "'");
									found = found || true;
								} else {
									/* UPDATE */
									repStore.update(keyRecv, valueRecv);
									logger.debug("[To Client] >>> Propagate-update '" + keyRecv + "'");
									found = found || true;
								}
							} else if (statusRecv == StatusType.S2S_SERVER_TABLE_PUT) {
								/* M4, TABLE PUT */
								if (repStore.containsKey(keyRecv)) {
									// add to existing table
									String table_string = repStore.get(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									table = solveTablePut(table, valueRecv);
									table_string = convertTableToString(table);
									repStore.put(keyRecv, table_string);
								} else {
									// create a new table
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = new ConcurrentHashMap<>();
									table = solveTablePut(table, valueRecv);
									String table_string = convertTableToString(table);
									repStore.put(keyRecv, table_string);
								}
								logger.debug("[To Client] >>> Propagate-table_put '" + keyRecv + "'");
								found = found || true;
							} else if (statusRecv == StatusType.S2S_SERVER_TABLE_DELETE) {
								/* M4, TABLE DELETE */
								if (repStore.containsKey(keyRecv)) {
									// delete from existing table
									String table_string = repStore.get(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									table = solveTableDelete(table, valueRecv);
									// if it is empty, delete this KV pair
									if (table.size() == 0) {
										repStore.delete(keyRecv);
									} else {
										table_string = convertTableToString(table);
										repStore.put(keyRecv, table_string);
									}
									logger.debug("[To Client] >>> Propagate-table_delete '" + keyRecv + "'");
									found = found || true;
								} else {
									// table not found
									found = found || false;
								}
							}
						}
					}
					if (!found) {
						replicas = null;
						logger.debug("[To Client] >>> Cannot find Replica for PUT/TABLE_PUT/TABLE_DELETE propagation, "
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
						thisReplica = entry.getValue();
						if (isBounded(hashedKey,
									  thisReplica.rangeFrom_Replica,
									  thisReplica.rangeTo_Replica)) {
							Store repStore = thisReplica.store;
							/* M4 addition */
							if (statusRecv == StatusType.S2S_SERVER_PUT) {
								if (valueRecv == null) {
									/* DELETE */
									if (repStore.containsKey(keyRecv)) {
										repStore.delete(keyRecv);
										logger.debug("[To Client] >>> Propagate-deleted '" + keyRecv + "'");
										found = found || true;
									} else {
										found = found || false;
									}
								} else if (!repStore.containsKey(keyRecv)) {
									/* PUT */
									repStore.put(keyRecv, valueRecv);
									logger.debug("[To Client] >>> Propagate-put '" + keyRecv + "'");
									found = found || true;
								} else {
									/* UPDATE */
									repStore.update(keyRecv, valueRecv);
									logger.debug("[To Client] >>> Propagate-update '" + keyRecv + "'");
									found = found || true;
								}
							} else if (statusRecv == StatusType.S2S_SERVER_TABLE_PUT) {
								/* M4, TABLE PUT */
								if (repStore.containsKey(keyRecv)) {
									// add to existing table
									String table_string = repStore.get(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									table = solveTablePut(table, valueRecv);
									table_string = convertTableToString(table);
									repStore.put(keyRecv, table_string);
								} else {
									// create a new table
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = new ConcurrentHashMap<>();
									table = solveTablePut(table, valueRecv);
									String table_string = convertTableToString(table);
									repStore.put(keyRecv, table_string);
								}
								logger.debug("[To Client] >>> Propagate-table_put '" + keyRecv + "'");
								found = found || true;
							} else if (statusRecv == StatusType.S2S_SERVER_TABLE_DELETE) {
								/* M4, TABLE DELETE */
								if (repStore.containsKey(keyRecv)) {
									// delete from existing table
									String table_string = repStore.get(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									table = solveTableDelete(table, valueRecv);
									// if it is empty, delete this KV pair
									if (table.size() == 0) {
										repStore.delete(keyRecv);
									} else {
										table_string = convertTableToString(table);
										repStore.put(keyRecv, table_string);
									}
									logger.debug("[To Client] >>> Propagate-table_delete '" + keyRecv + "'");
									found = found || true;
								} else {
									// table not found
									found = found || false;
								}
							}
						}
					}
					if (!found) {
						logger.error("[To Client] >>> Cannot find corresponding Replica even after "
									+ "re-initialization for PUT propagation.");
					}
				} catch (Exception e) {
					logger.error("[To Client] >>> Exception during receiving PUT/TABLE_PUT/TABLE_DELETE propagation.", e);
				} finally {
					close();
					return;
				}
			}
			
			// 4. normal KV response to client
			switch (statusRecv) {
				/* M4 */
				case TABLE_GET:
				case TABLE_SELECT:
				case GET: {
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (ptrKVServer.isCoordinatorForKey(keyRecv)) {
						/* Coordinator is responsible, check if Coordinator has it */
						if (!ptrKVServer.inStorage(keyRecv)) {
							/*
							 * first time not in storage,
							 * re-init Coordinator store and retry
							 */
							SerStatus serStatus_Copy = serStatus;
							logger.debug("[To Client] >>> GET/TABLE_GET/TABLE_SELECT key not in Coordinator store, "
										+ "reinitialize Coordinator store in case of unknown file transfer.");
							ptrKVServer.setSerStatus(SerStatus.STOPPED);
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							if (!ptrKVServer.reInitializeCoordinatorStore()) {
								/* Fatal error, the Store cannot be re-initialized. */
								ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
								kvMsgSend.setStatus(StatusType.GET_ERROR);
							} else {
								ptrKVServer.setSerStatus(serStatus_Copy);
								if (statusRecv == StatusType.GET) {
									if (ptrKVServer.inStorage(keyRecv)) {
										/* second time should be in storage, if exists */
										try {
											String valueSend = ptrKVServer.getKV(keyRecv);
											kvMsgSend.setValue(valueSend);
											kvMsgSend.setStatus(StatusType.GET_SUCCESS);
										} catch (Exception e) {
											logger.error("[To Client] >>> Exception in second-try GET.", e);
											kvMsgSend.setValue(null);
											kvMsgSend.setStatus(StatusType.GET_ERROR);
										}
									} else {
										/*
										* Key does not exist in storage,
										* reply GET with 'get error' and Key
										*/
										kvMsgSend.setStatus(StatusType.GET_ERROR);
									}
								/* M4 */
								} else if (statusRecv == StatusType.TABLE_GET) {
									if (!ptrKVServer.inStorage(keyRecv)) {
										kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
										kvMsgSend.setKey(keyRecv);
										kvMsgSend.setValue(valueRecv);
									} else {
										try {
											String table_string = ptrKVServer.getKV(keyRecv);
											ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
											String valueSend = solveTableCell(table, valueRecv);
											if (valueRecv.equals(valueSend)) {
												// TABLE_GET not found
												kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
											} else {
												// TABLE_GET hit
												kvMsgSend.setStatus(StatusType.TABLE_GET_SUCCESS);
											}
											kvMsgSend.setKey(keyRecv);
											kvMsgSend.setValue(valueSend);
										} catch (Exception e) {
											logger.error("[To Client] >>> Exception in second-try TABLE_GET.", e);
											kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
											kvMsgSend.setKey(keyRecv);
											kvMsgSend.setValue(valueRecv);
										}
									}
								/* M4 */
								} else if (statusRecv == StatusType.TABLE_SELECT) {
									if (!ptrKVServer.inStorage(keyRecv)) {
										kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
										kvMsgSend.setKey(keyRecv);
										kvMsgSend.setValue(valueRecv);
									} else {
										try {
											String table_string = ptrKVServer.getKV(keyRecv);
											ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
											String valueSend = solveTableSelect(table, valueRecv);
											if (valueRecv.equals(valueSend)) {
												// TABLE_SELECT not found
												kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
											} else {
												// TABLE_SELECT hit
												kvMsgSend.setStatus(StatusType.TABLE_SELECT_SUCCESS);
											}
											kvMsgSend.setKey(keyRecv);
											kvMsgSend.setValue(valueSend);
										} catch (Exception e) {
											logger.error("[To Client] >>> Exception in second-try TABLE_SELECT.", e);
											kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
											kvMsgSend.setKey(keyRecv);
											kvMsgSend.setValue(valueRecv);
										}
									}
								}
							}
							if (!sendKVMessage(kvMsgSend, output)) {
								close();
								return;
							}
							continue;
						}
						if (statusRecv == StatusType.GET) {
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
						/* M4 */
						} else if (statusRecv == StatusType.TABLE_GET) {
							try {
								if (!ptrKVServer.inStorage(keyRecv)) {
									kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
									kvMsgSend.setKey(keyRecv);
									kvMsgSend.setValue(valueRecv);
								} else {
									String table_string = ptrKVServer.getKV(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									String valueSend = solveTableCell(table, valueRecv);
									if (valueRecv.equals(valueSend)) {
										// TABLE_GET not found
										kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
									} else {
										// TABLE_GET hit
										kvMsgSend.setStatus(StatusType.TABLE_GET_SUCCESS);
									}
									kvMsgSend.setKey(keyRecv);
									kvMsgSend.setValue(valueSend);
								}
								if (!sendKVMessage(kvMsgSend, output)) {
									close();
									return;
								}
								continue;
							} catch (Exception e) {
								logger.error("TABLE_GET processing error at child of server #"
											+ serverPort+ " connected to IP: '"
											+ responseIP + "' \t port: "+ responsePort, e);
								kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
								if (!sendKVMessage(kvMsgSend, output)) {
									close();
									return;
								}
								continue;
							}
						/* M4 */
						} else if (statusRecv == StatusType.TABLE_SELECT) {
							try {
								if (!ptrKVServer.inStorage(keyRecv)) {
									kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
									kvMsgSend.setKey(keyRecv);
									kvMsgSend.setValue(valueRecv);
								} else {
									String table_string = ptrKVServer.getKV(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									String valueSend = solveTableSelect(table, valueRecv);
									if (valueRecv.equals(valueSend)) {
										// TABLE_SELECT not found
										kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
									} else {
										// TABLE_SELECT hit
										kvMsgSend.setStatus(StatusType.TABLE_SELECT_SUCCESS);
									}
									kvMsgSend.setKey(keyRecv);
									kvMsgSend.setValue(valueSend);
								}
								if (!sendKVMessage(kvMsgSend, output)) {
									close();
									return;
								}
								continue;
							} catch (Exception e) {
								logger.error("TABLE_SELECT processing error at child of server #"
											+ serverPort+ " connected to IP: '"
											+ responseIP + "' \t port: "+ responsePort, e);
								kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
								if (!sendKVMessage(kvMsgSend, output)) {
									close();
									return;
								}
								continue;
							}
						}
					} else {
						/* need to search in replicas */
						BigInteger hashedKey = hash(keyRecv);
						try {
							ConcurrentHashMap<Integer,Replica> replicas = ptrKVServer.getReplicas();
							boolean found = false;
							String value = null;
							Replica thisReplica;
							Iterator<Map.Entry<Integer,Replica>> iter = replicas.entrySet().iterator();
							while (iter.hasNext()) {
								Map.Entry<Integer,Replica> entry = iter.next();
								thisReplica = entry.getValue();
								/* first, check if any replica is outdated, if so delete them */
								if ((!ptrKVServer.getMetadata().contains(thisReplica.port+""))
									|| (isBounded(thisReplica.rangeFrom_Replica,
										 	      ptrKVServer.rangeFrom_Coordinator,
											      ptrKVServer.rangeTo_Coordinator))
									|| (!isBounded(thisReplica.rangeFrom_Replica,
												   ptrKVServer.rangeFrom_AllReplicas,
												   ptrKVServer.rangeTo_AllReplicas))) {
									// this Replica has been outdated, remove this
									logger.debug("[To Client] >>> Deleting an outdated Replica #"
												+ thisReplica.port + "...");
									String folderRemove = thisReplica.dirStore;
									File foldRemove = new File(folderRemove);
									if (foldRemove.exists()) {
										try {
											File[] files = foldRemove.listFiles();
											for (File file : files) {
												file.delete();
											}
											foldRemove.delete();
										} catch (Exception e) {
											logger.error("[To Client] >>> Exception when deleting outdated store files.", e);
										}
									}
									iter.remove();
									continue;
								}
								
								if (isBounded(hashedKey,
											  thisReplica.rangeFrom_Replica,
											  thisReplica.rangeTo_Replica)) {
									Store repStore = thisReplica.store;
									String valueTmp = repStore.get(keyRecv);
									if (valueTmp == null) {
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
									thisReplica = entry.getValue();
									if (isBounded(hashedKey,
												thisReplica.rangeFrom_Replica,
												thisReplica.rangeTo_Replica)) {
										Store repStore = thisReplica.store;
										String valueTmp = repStore.get(keyRecv);
										if (valueTmp == null) {
											found = found || false;
											continue;
										}
										found = found || true;
										value = valueTmp;
										continue;
									}
								}
							}
							if (statusRecv == StatusType.GET) {
								kvMsgSend.setValue(value);
								if (value == null) {
									kvMsgSend.setStatus(StatusType.GET_ERROR);
								} else {
									kvMsgSend.setStatus(StatusType.GET_SUCCESS);
								}
							} else if (statusRecv == StatusType.TABLE_GET) {
								ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(value);
								String valueSend = solveTableCell(table, valueRecv);
								if (valueRecv.equals(valueSend)) {
									// TABLE_GET not found
									kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
								} else {
									// TABLE_GET hit
									kvMsgSend.setStatus(StatusType.TABLE_GET_SUCCESS);
								}
								kvMsgSend.setKey(keyRecv);
								kvMsgSend.setValue(valueSend);
							} else if (statusRecv == StatusType.TABLE_SELECT) {
								ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(value);
								String valueSend = solveTableSelect(table, valueRecv);
								if (valueRecv.equals(valueSend)) {
									// TABLE_SELECT not found
									kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
								} else {
									// TABLE_SELECT hit
									kvMsgSend.setStatus(StatusType.TABLE_SELECT_SUCCESS);
								}
								kvMsgSend.setKey(keyRecv);
								kvMsgSend.setValue(valueSend);
							}
							if (!sendKVMessage(kvMsgSend, output)) {
								close();
								return;
							}
							continue;
						} catch (Exception e) {
							if (statusRecv == StatusType.GET) {
								logger.error("[To Client] >>> Exception during GET in Replicas.", e);
								kvMsgSend = new KVMessage();
								kvMsgSend.setStatus(StatusType.GET_ERROR);
								kvMsgSend.setKey(keyRecv);
							} else if (statusRecv == StatusType.TABLE_GET) {
								logger.error("[To Client] >>> Exception during TABLE_GET in Replicas.", e);
								kvMsgSend = new KVMessage();
								kvMsgSend.setStatus(StatusType.TABLE_GET_FAILURE);
								kvMsgSend.setKey(keyRecv);
								kvMsgSend.setValue(valueRecv);
							} else if (statusRecv == StatusType.TABLE_SELECT) {
								logger.error("[To Client] >>> Exception during TABLE_SELECT in Replicas.", e);
								kvMsgSend = new KVMessage();
								kvMsgSend.setStatus(StatusType.TABLE_SELECT_FAILURE);
								kvMsgSend.setKey(keyRecv);
								kvMsgSend.setValue(valueRecv);
							}
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
							 * first time not in storage,
							 * re-init Coordinator store and retry
							 */
							SerStatus serStatus_Copy = serStatus;
							logger.debug("[To Client] >>> DELETE key not in Coordinator store, "
										+ "reinitialize Coordinator store in case of unknown file transfer.");
							ptrKVServer.setSerStatus(SerStatus.STOPPED);
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
							if (!ptrKVServer.reInitializeCoordinatorStore()) {
								/* Fatal error, the Store cannot be re-initialized. */
								ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
								kvMsgSend.setStatus(StatusType.DELETE_ERROR);
							} else {
								ptrKVServer.setSerStatus(serStatus_Copy);
								if (ptrKVServer.inStorage(keyRecv)) {
									/* second time should be in storage, if exists */
									try {
										String value = ptrKVServer.getKV(keyRecv);
										kvMsgSend.setValue(value);
										ptrKVServer.putKV(keyRecv, null);
										kvMsgSend.setStatus(StatusType.DELETE_SUCCESS);
									} catch (Exception e) {
										logger.error("[To Client] >>> Exception in second-try DELETE.", e);
										kvMsgSend.setStatus(StatusType.DELETE_ERROR);
									}
								} else {
									/*
									* KV pair requested to delete does not exist,
									* reply PUT(DELETE) with 'delete success' and Key
									*/
									kvMsgSend.setStatus(StatusType.DELETE_ERROR);
								}
							}
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
				/* M4 */
				case TABLE_DELETE: {
					// propagate client's TABLE_DELETE request to servers with this Coordinator's Replicas
					if (!propagateTableDelete(keyRecv, valueRecv)) {
						logger.error("[To Client] >>> TABLE_DELETE propagation unsuccessful. Continue on.");
					}
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					if (!ptrKVServer.inStorage(keyRecv)) {
						/*
							* first time not in storage,
							* re-init Coordinator store and retry
							*/
						SerStatus serStatus_Copy = serStatus;
						logger.debug("[To Client] >>> TABLE_DELETE key not in Coordinator store, "
									+ "reinitialize Coordinator store in case of unknown file transfer.");
						ptrKVServer.setSerStatus(SerStatus.STOPPED);
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
						if (!ptrKVServer.reInitializeCoordinatorStore()) {
							/* Fatal error, the Store cannot be re-initialized. */
							ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
							kvMsgSend.setStatus(StatusType.TABLE_DELETE_FAILURE);
							kvMsgSend.setValue(valueRecv);
						} else {
							ptrKVServer.setSerStatus(serStatus_Copy);
							if (ptrKVServer.inStorage(keyRecv)) {
								/* second time should be in storage, if exists */
								try {
									String table_string = ptrKVServer.getKV(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									String value = solveTableCell(table, valueRecv);
									if (value.equals(valueRecv)) {
										kvMsgSend.setStatus(StatusType.TABLE_DELETE_FAILURE);
										kvMsgSend.setValue(valueRecv);
									} else {
										table = solveTableDelete(table, valueRecv);
										table_string = convertTableToString(table);
										if (table_string.equals("")) {
											ptrKVServer.putKV(keyRecv, null);
										} else {
											ptrKVServer.putKV(keyRecv, table_string);
										}
										kvMsgSend.setStatus(StatusType.TABLE_DELETE_SUCCESS);
										kvMsgSend.setValue(value);
									}
								} catch (Exception e) {
									logger.error("[To Client] >>> Exception in second-try DELETE.", e);
									kvMsgSend.setStatus(StatusType.DELETE_ERROR);
								}
							} else {
								/*
								* KV pair requested to delete does not exist,
								* reply TABLE_DELETE with 'delete success' and Key
								*/
								kvMsgSend.setStatus(StatusType.TABLE_DELETE_FAILURE);
								kvMsgSend.setValue(valueRecv);
							}
						}
						if (!sendKVMessage(kvMsgSend, output)) {
							close();
							return;
						}
						continue;
					}
					try {
						String table_string = ptrKVServer.getKV(keyRecv);
						ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
						String value = solveTableCell(table, valueRecv);
						if (value.equals(valueRecv)) {
							kvMsgSend.setStatus(StatusType.TABLE_DELETE_FAILURE);
							kvMsgSend.setValue(valueRecv);
						} else {
							table = solveTableDelete(table, valueRecv);
							table_string = convertTableToString(table);
							if (table_string.equals("")) {
								ptrKVServer.putKV(keyRecv, null);
							} else {
								ptrKVServer.putKV(keyRecv, table_string);
							}
							kvMsgSend.setStatus(StatusType.TABLE_DELETE_SUCCESS);
							kvMsgSend.setValue(value);
						}
						if (!sendKVMessage(kvMsgSend, output)) {
							close();
							return;
						}
						continue;
					} catch (Exception e) {
						logger.error("TABLE_DELETE processing error at child of "
									+ "server #" + serverPort+ " connected to IP: '"
									+ responseIP + "' \t port: "+ responsePort, e);
						kvMsgSend.setStatus(StatusType.TABLE_DELETE_FAILURE);
						kvMsgSend.setValue(valueRecv);
						if (!sendKVMessage(kvMsgSend, output)) {
							close();
							return;
						}
						continue;
					}
				}
				/* M4 */
				case TABLE_PUT: {
					// propagate client's TABLE_PUT request to servers with this Coordinator's Replicas
					if (!propagateTablePut(keyRecv, valueRecv)) {
						logger.error("[To Client] >>> TABLE_PUT propagation unsuccessful. Continue on.");
					}
					KVMessage kvMsgSend = new KVMessage();
					kvMsgSend.setKey(keyRecv);
					kvMsgSend.setValue(valueRecv);
					if (!ptrKVServer.inStorage(keyRecv)) {
						SerStatus serStatus_Copy = serStatus;
						logger.debug("[To Client] >>> TABLE_PUT key not in Coordinator store, "
									+ "reinitialize Coordinator store in case of unknown file transfer.");
						ptrKVServer.setSerStatus(SerStatus.STOPPED);
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
						if (!ptrKVServer.reInitializeCoordinatorStore()) {
							/* Fatal error, the Store cannot be re-initialized. */
							ptrKVServer.setSerStatus(SerStatus.SHUTTING_DOWN);
							kvMsgSend.setStatus(StatusType.TABLE_PUT_FAILURE);
						} else {
							ptrKVServer.setSerStatus(serStatus_Copy);
							/* second time should be in storage, if exists */
							if (ptrKVServer.inStorage(keyRecv)) {
								try {
									String table_string = ptrKVServer.getKV(keyRecv);
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
									table = solveTablePut(table, valueRecv);
									table_string = convertTableToString(table);
									ptrKVServer.putKV(keyRecv, table_string);
									kvMsgSend.setStatus(StatusType.TABLE_PUT_SUCCESS);
								} catch (Exception e) {
									logger.error("[To Client] >>> Exception in second-try TABLE_PUT.", e);
									kvMsgSend.setStatus(StatusType.TABLE_PUT_FAILURE);
								}
							} else {
								try {
									ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = new ConcurrentHashMap<>();
									table = solveTablePut(table, valueRecv);
									String table_string = convertTableToString(table);
									ptrKVServer.putKV(keyRecv, table_string);
									kvMsgSend.setStatus(StatusType.TABLE_PUT_SUCCESS);
								} catch (Exception e) {
									logger.error("[To Client] >>> Exception in second-try TABLE_PUT.", e);
									kvMsgSend.setStatus(StatusType.TABLE_PUT_FAILURE);
								}
							}
						}
						if (!sendKVMessage(kvMsgSend, output)) {
							close();
							return;
						}
						continue;
					}
					try {
						String table_string = ptrKVServer.getKV(keyRecv);
						ConcurrentHashMap<String, ConcurrentHashMap<String, String>> table = convertStringToTable(table_string);
						table = solveTablePut(table, valueRecv);
						table_string = convertTableToString(table);
						ptrKVServer.putKV(keyRecv, table_string);
						kvMsgSend.setStatus(StatusType.TABLE_PUT_SUCCESS);
						if (!sendKVMessage(kvMsgSend, output)) {
							close();
							return;
						}
						continue;
					} catch (Exception e) {
						logger.error("TABLE_PUT processing error at child of "
									+ "server #" + serverPort+ " connected to IP: '"
									+ responseIP + "' \t port: "+ responsePort, e);
						kvMsgSend.setStatus(StatusType.TABLE_PUT_FAILURE);
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

	/* M4 */
	private boolean propagateTableDelete(String key, String value) {
		try {
			boolean isSuccess = true;
			String meta = ptrKVServer.getMetadata();
			int portSuccessor = findSuccessorPort(meta, serverPort);
			if (portSuccessor == serverPort) {
				// Case: 1 server
				return isSuccess;
			}
			// Cases: 2+ servers
			logger.debug("[To Client] >>> propagate TABLE_DELETE to #" + portSuccessor);
			isSuccess = isSuccess && connectAndServerTableDelete(portSuccessor, key, value);
			int portSSuccessor = findSuccessorPort(meta, portSuccessor);
			if (portSSuccessor == serverPort) {
				// Case: 2 servers
				return isSuccess;
			}
			// Cases: 3+ servers
			logger.debug("[To Client] >>> propagate TABLE_DELETE to #" + portSSuccessor);
			isSuccess = isSuccess && connectAndServerTableDelete(portSSuccessor, key, value);
			return isSuccess;
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during TABLE_DELETE propagation. ", e);
			return false;
		}
	}

	/* M4 */
	private boolean connectAndServerTableDelete(int port, String key, String value) {
		try {
			Socket tSock = new Socket(responseIP, port);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			KVMessage kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2S_SERVER_TABLE_DELETE);
			kvMsgSend.setKey(key);
			kvMsgSend.setValue(value);
			if (!sendKVMessage(kvMsgSend, tOutput)) {
				logger.error("[To Client] >>> Cannot contact #" + port + " to propagate TABLE_DELETE.");
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
				logger.debug("[To Client] >>> Finish TABLE_DELETE propagation to server #" + port + ".");
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
				return true;
			}
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during connection for propagating TABLE_DELETE.", e);
		}
		return false;
	}

	/* M4 */
	private boolean propagateTablePut(String key, String value) {
		try {
			boolean isSuccess = true;
			String meta = ptrKVServer.getMetadata();
			int portSuccessor = findSuccessorPort(meta, serverPort);
			if (portSuccessor == serverPort) {
				// Case: 1 server
				return isSuccess;
			}
			// Cases: 2+ servers
			logger.debug("[To Client] >>> propagate TABLE_PUT to #" + portSuccessor);
			isSuccess = isSuccess && connectAndServerTablePut(portSuccessor, key, value);
			int portSSuccessor = findSuccessorPort(meta, portSuccessor);
			if (portSSuccessor == serverPort) {
				// Case: 2 servers
				return isSuccess;
			}
			// Cases: 3+ servers
			logger.debug("[To Client] >>> propagate TABLE_PUT to #" + portSSuccessor);
			isSuccess = isSuccess && connectAndServerTablePut(portSSuccessor, key, value);
			return isSuccess;
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during TABLE_PUT propagation. ", e);
			return false;
		}
	}

	/* M4 */
	private boolean connectAndServerTablePut(int port, String key, String value) {
		try {
			Socket tSock = new Socket(responseIP, port);
			tSock.setReuseAddress(true);
			DataInputStream tInput = new DataInputStream(new BufferedInputStream(tSock.getInputStream()));
			DataOutputStream tOutput = new DataOutputStream(new BufferedOutputStream(tSock.getOutputStream()));
			KVMessage kvMsgSend = new KVMessage();
			kvMsgSend.setStatus(StatusType.S2S_SERVER_TABLE_PUT);
			kvMsgSend.setKey(key);
			kvMsgSend.setValue(value);
			if (!sendKVMessage(kvMsgSend, tOutput)) {
				logger.error("[To Client] >>> Cannot contact #" + port + " to propagate TABLE_PUT.");
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
				logger.debug("[To Client] >>> Finish TABLE_PUT propagation to server #" + port + ".");
			} finally {
				tSock.close();
				tSock = null;
				tInput = null;
				tOutput = null;
				return true;
			}
		} catch (Exception e) {
			logger.error("[To Client] >>> Exception during connection for propagating TABLE_PUT.", e);
		}
		return false;
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
