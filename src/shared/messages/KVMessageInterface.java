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
		
		GET_KEYRANGE,		/* client -> server: request for latest Keyrange metadata */

		NOT_SET,			/* Custom: placeholder for new KVMessage */


		// a new server starting
		S2E_INIT_REQUEST_WITH_DIR, 			/* Server -> ECS:  a new server's first message to ECS, with disk directory in Value */
		E2S_INIT_RESPONSE_WITH_META, 		/* ECS -> Server:  response to prior message, with keyrange metadata in Value */
		E2S_COMMAND_SERVER_RUN, 			/* ECS -> Server:  after ECS sent E2S_INIT_RESPONSE_WITH_META, ECS sent this message to the server to set its status to RUNNING. */

		// following messages used within while loop
		E2S_EMPTY_CHECK, 					/* ECS -> Server:  after last ECS receiveMessage(), if ECS has no command or update for this server, after a certain sleep time, ECS will send this message with no Key or Value. */
		S2E_EMPTY_RESPONSE, 				/* Server -> ECS:  after last server receiveMessage(), if the server is not going to shutdown and is not required to respond with specific message, the server will send this message with no Key or Value. */
		S2E_SHUTDOWN_REQUEST, 				/* Server -> ECS:  after last server receiveMessage(), if the server is going to shutdown, the server will send this message with no Key or Value. */
		E2S_WRITE_LOCK, 					/* ECS -> Server:  after receiving S2E_REQUEST_SHUTDOWN, ECS sends write lock to removed node but data transfer is invoked on successor node */
		E2S_WRITE_LOCK_WITH_KEYRANGE, 		/* ECS -> Server:  after receiving S2E_REQUEST_SHUTDOWN or accepting a new connection, ECS does not need to sleep, and will send this message to the corresponding server with Value being a single String: "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port" */
		S2E_FINISHED_FILE_TRANSFER, 		/* Server -> ECS:  after receiving E2S_WRITE_LOCK_WITH_KEYRANGE, the server will send this message (no key, no value) to ECS after it finishes file transfer, write lock is not removed yet. For shutting-down server, this will be the last message. */
		E2S_UPDATE_META_AND_RUN 			/* ECS -> Server:  after receiving S2E_FINISHED_FILE_TRANSFER, the server does not need to sleep, and will send this message (value being metadata) to the server to update its keyrange metadata and remove its write lock. */
											/*                 this message can also be sent to a server when a new server connection is accepted to update its keyrange metadata. */
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
	 */
	public void logMessageContent();
}
