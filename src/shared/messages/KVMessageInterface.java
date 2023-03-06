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
		// or, a server shutting down:
		S2E_SHUTDOWN_REQUEST, 				/* Server -> ECS:  a server, that has disconnect and re-connect with ECS in order to request its owm shutdown, sends this message as its first message to ECS since the new connection. */
		// note:  for the server to take the initiative (avoid blocked by its own receiveMessage() waiting for the following ECS commands), when a server needs to shut down, it will disconnect from ECS first then reconnect to ECS with the prior S2E_SHUTDOWN_REQUEST message.


		E2S_COMMAND_SERVER_RUN, 			/* ECS -> Server:  since a new server starts with STOPPED status, ECS sends this message (without key or value) to the server to set it RUNNING to accept client requests. */
		 									/*                 for this message, the server does not respond ECS. Therefore, after ECS sends E2S_COMMAND_SERVER_RUN, ECS must NOT call receiveMessage(). */

		E2S_WRITE_LOCK_WITH_KEYRANGE, 		/* ECS -> Server:  when a new server connects to ECS, ECS sends this to other keyrange-changing servers that need to transfer files and be Locked for Write, with Value being a single String "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port" */
											/*                 this message is also used by ECS to respond to S2E_SHUTDOWN_REQUEST in order to Lock Write of the server that requested shut down. */
		S2E_FINISHED_FILE_TRANSFER, 		/* Server -> ECS:  after the server finished file transfer, the server responds to prior message with this. No key or value. (Write Lock has not been released)*/
											/*                 if the server has requested to shutdown itself before, the server will actually shutdown after sending this message to ECS. */

		E2S_UPDATE_META_AND_RUN, 			/* ECS -> Server:  ECS takes the initiative to send this message to a server in order to update its keyrange metadata, with full keyrange metadata in Value. Once the server receives it, no matter if the server is Locked for Write, it will become RUNNING.*/
											/*                 for this message, the server does not respond ECS. Therefore, after ECS sends E2S_UPDATE_META_AND_RUN, ECS must NOT call receiveMessage(). */
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
