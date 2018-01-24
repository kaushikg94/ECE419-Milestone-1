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
				logger.error("Invalid request type for serialization: " +
					requestType);
				return "";
		}
	}

	/**
	 * Unserializes the given string into a KVMessage
	 * @param lines the list of lines in the message to parse
	 * 				(excluding trailing blank line at end)
	 * @return the unserialized KVMessage
	 */
	public static KVMessage unserialize(String[] lines)
			throws IllegalArgumentException {
		if(lines.length < 1) {
			throw new IllegalArgumentException("Cannot process empty request");
		}
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
			
			// Unrecognized
			default:
				throw new IllegalArgumentException("Invalid status type " +
					"when unserializing request: '" + lines[0] + "'");
		}
	}

	public static KVMessage parseGetRequest(String[] lines)
			throws IllegalArgumentException {
		if(lines.length != 2) {
			throw new IllegalArgumentException("Invalid number of arguments " +
				"when unserializing GET request: " + lines.length);
		}

		return new KVMessageImpl(lines[1], null, lines[0]);
	}

	public static KVMessage parsePutRequest(String[] lines)
			throws IllegalArgumentException {
		if(lines.length < 2 || lines.length > 3) {
			throw new IllegalArgumentException("Invalid number of arguments " +
				"when unserializing PUT request: " + lines.length);
		}

		return new KVMessageImpl(lines[1],
			lines.length == 3 ? lines[2] : null, lines[0]);
	}

	public static KVMessage parseSuccessResponse(String[] lines)
			throws IllegalArgumentException {
		if(lines.length != 3) {
			throw new IllegalArgumentException("Invalid number of arguments " +
				"when unserializing success response: " + lines.length);
		}

		return new KVMessageImpl(lines[1], lines[2], lines[0]);
	}

	public static KVMessage parseErrorResponse(String[] lines)
			throws IllegalArgumentException {
		if(lines.length != 2) {
			throw new IllegalArgumentException("Invalid number of arguments " +
				"when unserializing error response: " + lines.length);
		}

		return new KVMessageImpl(null, lines[1], lines[0]);
	}
}
