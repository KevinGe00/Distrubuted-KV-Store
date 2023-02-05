package shared.messages;

public class KVMessageObj implements KVMessage{
	private StatusType status;
	private String key;
	private String value;

	public KVMessageObj() { }

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
		return;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value){
		this.value = value;
		return;
	}
	
	public StatusType getStatus() {
		return status;
	}
	
	public void setStatus(StatusType status) {
		this.status = status;
		return;
	}
}
