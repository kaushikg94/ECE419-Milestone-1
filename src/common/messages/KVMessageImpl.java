package common.messages;

import org.apache.log4j.*;

public class KVMessageImpl implements KVMessage {

	private static Logger logger = Logger.getRootLogger();

	String key;
	String value;
	StatusType status;

	public KVMessageImpl(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}

	public KVMessageImpl(String key, String value, String status) {
		this.key = key;
		this.value = value;
		switch(status) {
			case "GET":
				this.status = StatusType.GET;
				break;
			case "GET_ERROR":
				this.status = StatusType.GET_ERROR;
				break;
			case "GET_SUCCESS":
				this.status = StatusType.GET_SUCCESS;
				break;
			case "PUT":
				this.status = StatusType.PUT;
				break;
			case "PUT_SUCCESS":
				this.status = StatusType.PUT_SUCCESS;
				break;
			case "PUT_UPDATE":
				this.status = StatusType.PUT_UPDATE;
				break;
			case "PUT_ERROR":
				this.status = StatusType.PUT_ERROR;
				break;
			case "DELETE_SUCCESS":
				this.status = StatusType.DELETE_SUCCESS;
				break;
			case "DELETE_ERROR":
				this.status = StatusType.DELETE_ERROR;
				break;
			default:
				logger.warn("Invalid status type: " + status);
		}
	}

	@Override
	public String getKey() {
		return key;
	}
	
	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}
}
