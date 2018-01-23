package serialization;

import org.apache.log4j.*;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVMessage.StatusType;


/**
 * Handles serialization and unserialization of requests and responses.
 */
public class Serialization {

	private static Logger logger = Logger.getRootLogger();

	private static final int BUFFER_SIZE = 122880;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	/**
	 * Serializes the given KVMessage into a string
	 * @param message the KVMessage to serialize
	 * @return the serialized string message
	 */
	public static String serialize(KVMessage message) {
		StatusType requestType = message.getStatus();
		switch(requestType) {
			case GET:
				return "GET\n" + message.getKey() + "\n\n";
			case PUT:
				return "PUT\n" + message.getKey() + "\n" +
					message.getValue() + "\n\n";
			case GET_SUCCESS:
			case PUT_SUCCESS:
			case PUT_UPDATE:
			case DELETE_SUCCESS:
				return requestType + "\n" + message.getKey() + "\n" +
					message.getValue() + "\n\n";
			case GET_ERROR:
			case PUT_ERROR:
			case DELETE_ERROR:
				return requestType + "\n" + message.getValue() + "\n\n";
			default:
				logger.error("Error: invalid request type for " +
					"serialization: " + requestType);
				return "";
		}
	}

	/**
	 * Unserializes the given string into a KVMessage
	 * @param lines the list of lines in the message to parse
	 * 				(including trailing newline at end)
	 * @return the unserialized KVMessage
	 */
	public static KVMessage unserialize(String[] lines) throws Exception {
		switch(lines[0]) {
			// Requests
			case "GET":
				return parseGetRequest(lines);
			case "PUT":
				return parsePutRequest(lines);
			
			// Responses
			case "GET_SUCCESS":
			case "PUT_SUCCESS":
			case "PUT_UPDATE":
			case "DELETE_SUCCESS":
				return parseSuccessResponse(lines);
			case "GET_ERROR":
			case "PUT_ERROR":
			case "DELETE_ERROR":
				return parseErrorResponse(lines);
			
			default:
				logger.error("Error: Invalid status type: " + lines[0]);
				throw new Exception("Invalid status type");
		}
	}

	public static KVMessage parseGetRequest(String[] lines) throws Exception {
		if(lines.length != 3) {
			logger.error("Error: Invalid number of arguments during " +
				"unserialization of GET request");
			throw new Exception("Invalid number of arguments");
		}

		return new KVMessageImpl(lines[1], null, lines[0]);
	}

	public static KVMessage parsePutRequest(String[] lines) throws Exception {
		if(lines.length < 3 || lines.length > 4) {
			logger.error("Error: Invalid number of arguments during " +
				"unserialization of PUT request");
			throw new Exception("Invalid number of arguments");
		}

		return new KVMessageImpl(lines[1], lines.length == 4 ? lines[2] : null,
			lines[0]);
	}

	public static KVMessage parseSuccessResponse(String[] lines) throws Exception {
		if(lines.length != 4) {
			logger.error("Error: Invalid number of arguments during " +
				"unserialization of error success");
			throw new Exception("Invalid number of arguments");
		}

		return new KVMessageImpl(lines[1], lines[2], lines[0]);
	}

	public static KVMessage parseErrorResponse(String[] lines) throws Exception {
		if(lines.length != 3) {
			logger.error("Error: Invalid number of arguments during " +
				"unserialization of error response");
			throw new Exception("Invalid number of arguments");
		}

		return new KVMessageImpl(null, lines[2], lines[0]);
	}
}
