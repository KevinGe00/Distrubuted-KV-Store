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

		NOT_SET			/* Custom: placeholder for new KVMessage */
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
