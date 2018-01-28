package testing;

import org.junit.Test;
import junit.framework.TestCase;

import client.KVStore;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void testStub() {
		assertTrue(true);
	}
}
