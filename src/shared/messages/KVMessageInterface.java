package shared.messages;

public interface KVMessageInterface {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		
		SERVER_STOPPED,			/* Server stopped, no request processed */
		SERVER_WRITE_LOCK, 		/* Server locked writing, only get available */
		SERVER_NOT_RESPONSIBLE,	/* Request failed as this server is not responsible for this key */
		KEYRANGE_SUCCESS,		/* semi-colon seperated <hex_start,hex_end,IP:port> pairs */
		KEYRANGE_READ_SUCCESS, 	/* semi-colon seperated <hex_start,hex_end,IP:port> pairs */
		
		GET_KEYRANGE,		/* client -> server: request for latest Keyrange metadata */
		GET_KEYRANGE_READ, 	/* client -> server: request for latest Keyrange metadata (full) */

		NOT_SET,			/* Custom: placeholder for new KVMessage */


		// ================================================================================================================================
		/* M4:
		 * Table, Query + Derived Table (SELECT + FROM)
		 *  	> table as a KV pair: "key_name" "row_name_I
		 * 										  row_name_II
		 * 										  ...
		 * 										  row_name_X
		 * 										  
		 * 										  column_name_A
		 * 										  column_name_B
		 * 										  ...
		 * 										  column_name_Z
		 * 										  
		 * 										  value_I_A (can be empty)
		 * 										  value_I_B
		 * 										  ...
		 * 										  value_I_Z
		 * 										  value_II_A
		 * 										  value_II_B
		 * 										  ...
		 * 										  value_II_Z
		 * 										  ...
		 * 										  value_X_A
		 * 										  value_X_B
		 * 										  ...
		 * 										  value_X_Z" 						(connected with System.lineSeperator(), which can be '\n', '\r', or '\n\r')
		 *  	
		 * 
		 * 		> add/update cell in table: TABLE_PUT "key_name" "row_name_VII
		 * 												   		  column_name_G
		 * 												   		  value_VII_G" 		(^)
		 * 		> add/update success: TABLE_PUT_SUCCESS "key_name" "row_name_VII
		 * 															column_name_G
		 * 															value_VII_G" 	(^)
		 * 		> add/update failure: TABLE_PUT_FAILURE "key_name" "row_name_VII
		 * 															column_name_G
		 * 															value_VII_G" 	(^)
		 * 
		 * 
		 * 		> remove cell from table: TABLE_DELETE "key_name" "row_name_VII
		 * 														   column_name_G" 	(^)
		 * 		> remove success: TABLE_DELETE_SUCCESS "key_name" "row_name_VII
		 * 														   column_name_G
		 * 														   value_VII_G" 	(^)
		 * 		> remove failure: TABLE_DELETE_FAILURE "key_name" "row_name_VII
		 * 														   column_name_G" 	(^)
		 * 
		 * 
		 * 		> get cell from table: TABLE_GET "key_name" "row_name_VII
		 * 													 column_name_G" 		(^)
		 * 		> get success: TABLE_GET_SUCCESS "key_name" "row_name_VII
		 * 													 column_name_G
		 * 													 value_VII_G" 			(^)
		 * 		> get failure: TABLE_GET_FAILURE "key_name" "row_name_VII
		 * 													 column_name_G" 		(^)
		 * 
		 * 
		 * 		> add/update propagation: S2S_SERVER_TABLE_PUT "key_name" "row_name_VII
		 * 																   column_name_G
		 * 																   value_VII_G" 	(^)
		 * 		> delete propagation: S2S_SERVER_TABLE_DELETE "key_name" "row_name_VII
		 * 																  column_name_G" 	(^)
		 */
		TABLE_PUT,
		TABLE_PUT_SUCCESS,
		TABLE_PUT_FAILURE,
		
		TABLE_DELETE,
		TABLE_DELETE_SUCCESS,
		TABLE_DELETE_FAILURE,

		TABLE_GET,
		TABLE_GET_SUCCESS,
		TABLE_GET_FAILURE,

		S2S_SERVER_TABLE_PUT,
		S2S_SERVER_TABLE_DELETE,

		/* M4:
		 * table_select column_name_A,column_name_B>50,column_name_C<50 from key_name_1,key_name_2
		 * as TABLE_SELECT "key_name_1" "column_name_A
		 * 								 column_name_B>50
		 * 								 column_name_C<50" 		(^)
		 * and
		 * as TABLE_SELECT "key_name_2" "column_name_A
		 * 								 column_name_B>50
		 * 								 column_name_C<50" 		(^)
		 * 
		 * 
		 * for table "key_name_1":
		 * TABLE_SELECT_SUCCESS "key_name_1" "row_name_I
		 * 									  row_name_II
		 * 									  ...
		 * 									  row_name_X
		 * 
		 * 									  column_name_A
		 * 									  column_name_B
		 * 									  column_name_C
		 * 
		 * 									  value_I_A
		 * 									  value_I_B
		 * 									  value_I_C
		 * 									  value_II_A
		 * 									  value_II_B
		 * 									  value_II_C
		 * 									  ...
		 * 									  value_X_A
		 * 									  value_X_B
		 * 									  value_X_C" 		(^)
		 * or
		 * TABLE_SELECT_FAILURE "key_name_1" "column_name_A
		 * 									  column_name_B>50
		 * 									  column_name_C<50"
		 */
		TABLE_SELECT,
		TABLE_SELECT_SUCCESS,
		TABLE_SELECT_FAILURE,
		// ================================================================================================================================


		// a new server starting
		S2E_INIT_REQUEST_WITH_DIR, 			/* Server -> ECS:  a new server's first message to ECS, with disk directory in Value */
		E2S_INIT_RESPONSE_WITH_META, 		/* ECS -> Server:  response to prior message, with keyrange metadata in Value */
		E2S_COMMAND_SERVER_RUN, 			/* ECS -> Server:  after ECS sent E2S_INIT_RESPONSE_WITH_META, ECS sent this message to the server to set its status to RUNNING. */

		// following messages used within while loop
		E2S_EMPTY_CHECK, 					/* ECS -> Server:  after last ECS receiveMessage(), if ECS has no command or update for this server, after a certain sleep time, ECS will send this message with no Key or Value. */
		S2E_EMPTY_RESPONSE, 				/* Server -> ECS:  after last server receiveMessage(), if the server is not going to shutdown and is not required to respond with specific message, the server will send this message with no Key or Value. */
		S2E_SHUTDOWN_REQUEST, 				/* Server -> ECS:  after last server receiveMessage(), if the server is going to shutdown, the server will send this message with no Key or Value. */
		E2S_WRITE_LOCK_WITH_KEYRANGE, 		/* ECS -> Server:  after receiving S2E_REQUEST_SHUTDOWN or accepting a new connection, ECS does not need to sleep, and will send this message to the corresponding server with Value being a single String: "Directory_Receiver,NewBigInt_from,NewBigInt_to,receiver_IP,receiver_port" */
		S2A_FINISHED_FILE_TRANSFER, 		/* Server -> ECS:  after receiving E2S_WRITE_LOCK_WITH_KEYRANGE, the server will send this message (no key, no value) to ECS after it finishes file transfer, write lock is not removed yet. For shutting-down server, this will be the last message. */
											/* Server Sending -> Server Receiving: send this to the server it is transferring files to after it finishes */
		E2S_UPDATE_META_AND_RUN, 			/* ECS -> Server:  after receiving S2E_FINISHED_FILE_TRANSFER, the server does not need to sleep, and will send this message (value being metadata) to the server to update its keyrange metadata and remove its write lock. */
											/*                 this message can also be sent to a server when a new server connection is accepted to update its keyrange metadata. */
		
		// milestone 3 extension
		S2S_SERVER_PUT 						/* Server Coordinator -> Server Replica: after a client's PUT, the coordinator server will contact key's replica server(s) for the new KV pair. */
	}

	// set and get methods for Status
	/**
	 * set Status of this KVMessage; if not called, Status is NOT_SET
	 * @param status a KVMessageInterface.StatusType
	 * @return true if this operation succeed
	 */
	public boolean setStatus(StatusType status);
	/**
	 * get Status of this KVMessage
	 * @return a KVMessageInterface.StatusType
	 */
	public StatusType getStatus();

	// set and get methods for Key
	/**
	 * set Key of this KVMessage; if not called, Key is NULL
	 * @param key a String with a max length of 20
	 * @return true if this operation succeed
	 */
	public boolean setKey(String key);
	/**
	 * get Key of this KVMessage
	 * @return a String containing Key
	 */
	public String getKey();

	// set and get methods for Value
	/**
	 * set Value of this KVMessage, if not called, Value is NULL
	 * @param value a String with a max length of 120,000
	 * @return true if this operation succeed
	 */
	public boolean setValue(String value);
	/**
	 * get Value of this KVMessage
	 * @return a String containing Value
	 */
	public String getValue();

	// serialization and deserialization of KVMessage
	/**
	 * serialize this KVMessage into a compact byte[]
	 * @return a compact byte[] with protocol-specified structure
	 */
	public byte[] toBytes() throws Exception;
	/**
	 * deserialize a compact byte[] into this KVMessage
	 * @param bytes a compact byte[] with protocol-specified structure
	 * @return true if this operation succeed
	 */
	public boolean fromBytes(byte[] bytes) throws Exception;

	/**
	 * log this message's status, key, value (including null).
	 * @param true_for_receiving true for receiving, false for sending.
	 */
	public void logMessageContent(Boolean true_for_receiving);
}
