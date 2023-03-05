package shared.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.log4j.Logger;

public class KVMessage implements KVMessageInterface{
	private static Logger logger = Logger.getRootLogger();
	private StatusType status;
	private String key;
	private String value;

	public KVMessage() {
		status = StatusType.NOT_SET;
		key = null;
		value = null;
	}

	public boolean setStatus(StatusType status) {
		try {
			this.status = status;
			logger.debug("Set KVMessage's Status to: <" + status.name() + ">");
		} catch (Exception e) {
			logger.error("Fail to set KVMessage's Status.");
			return false;
		}
		return true;
	}

	public StatusType getStatus(){
		if (status == StatusType.NOT_SET) {
			logger.warn("Got Status <NOT_SET> from NOT_SET KVMessage.");
		}
		return status;
	}

	public boolean setKey(String key) {
		try {
			if ((key != null) && (key.length() > 20)) {
				logger.error("Key for KVMessage is too long: <" + key + ">");
				return false;
			}
			this.key = key;
			logger.debug("Set KVMessage's Key to: <" + key + ">");
		} catch (Exception e) {
			logger.error("Fail to set KVMessage's Key.");
			return false;
		}
		return true;
	}

	public String getKey() {
		if (status == StatusType.NOT_SET) {
			logger.warn("Got Key: <" + key + "> from NOT_SET KVMessage.");
		}
		return key;
	}

	public boolean setValue(String value) {
		try {
			if ((value != null) && (value.length() > 120000)) {
				logger.error("Value for KVMessage is too long: " + value.length());
				return false;
			}
			this.value = value;
			logger.debug("Set KVMessage's Value to: <" + value + ">");
		} catch (Exception e) {
			logger.error("Fail to set KVMessage's Value.");
			return false;
		}
		return true;
	}

	public String getValue() {
		if (status == StatusType.NOT_SET) {
			logger.warn("Got Value: <" + value + "> from NOT_SET KVMessage.");
		}
		return value;
	}

	public byte[] toBytes() throws Exception{
		ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();
		DataOutputStream dataOutStream = new DataOutputStream(bytesOutStream);
		// 1 Byte (Byte): KVMessage Status [<StatusType types]
		dataOutStream.writeByte(status.ordinal());
		// 1 Byte (Byte): KVMessage Key length [<20]
		// n Bytes (String): KVMessage Key [UTF-8]
		if (key == null) {
			dataOutStream.writeByte(0);
		} else {
			dataOutStream.writeByte(key.length());
			dataOutStream.write(key.getBytes("UTF-8"));
		}
		// 4 Bytes (Int): KVMessage Value length [<120,000]
		// m Bytes (String): KVMessage Value [UTF-8]
		if (value == null) {
			dataOutStream.writeInt(0);
		} else {
			dataOutStream.writeInt(value.length());
			dataOutStream.write(value.getBytes("UTF-8"));
		}
		dataOutStream.flush();
		return bytesOutStream.toByteArray();
	}

	public boolean fromBytes(byte[] bytes) {
		ByteArrayInputStream bytesInStream = new ByteArrayInputStream(bytes);
		DataInputStream dataInStream = new DataInputStream(bytesInStream);
		try {
			// 1 Byte (Byte): KVMessage Status [<StatusType types]
			status = StatusType.values()[dataInStream.readByte()];
			// 1 Byte (Byte): KVMessage Key length [<20]
			int size_key = dataInStream.readByte();
			// n Bytes (String): KVMessage Key [UTF-8]
			key = null;
			if (size_key > 0) {
				byte[] bytes_key = new byte[size_key];
				dataInStream.readFully(bytes_key);
				key = new String(bytes_key, "UTF-8");
			}
			// 4 Bytes (Int): KVMessage Value length [<120,000]
			int size_value = dataInStream.readInt();
			// m Bytes (String): KVMessage Value [UTF-8]
			value = null;
			if (size_value > 0) {
				byte[] bytes_value = new byte[size_value];
				dataInStream.readFully(bytes_value);
				value = new String(bytes_value, "UTF-8");
			}
		} catch (Exception e) {
			logger.error("Exception during conversion from bytes to KVMessage.", e);
			return false;
		}
		return true;
	}

	public void logMessageContent() {
		logger.info("<" + status.name() + "> "
					+ "<" + key + "> "
					+ "<" + value + ">");
	}
}
