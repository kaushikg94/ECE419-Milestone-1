package testing;

import java.io.FileNotFoundException;

import org.junit.Test;

import app_kvServer.KVServer;
import junit.framework.TestCase;
import app_kvServer.IKVServer.CacheStrategy;

public class ServerTest extends TestCase {

	private static final int PORT = 4000;
	private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.FIFO;
	private static final int CACHE_SIZE = 4;

	private KVServer kvServer;
	
	public void setUp() {
		kvServer = new KVServer(PORT, CACHE_STRATEGY, CACHE_SIZE);
		kvServer.clearStorage();
	}

	public void tearDown() {
		kvServer.clearStorage();
		kvServer.close();
	}
	
	@Test
	public void testGetSelfInfo() {
		String hostname = kvServer.getHostname();
		int port = kvServer.getPort();
		CacheStrategy cacheStrategy = kvServer.getCacheStrategy();
		int cacheSize = kvServer.getCacheSize();

		// Hostname is null since server not running
		assertTrue(hostname == null && port == PORT &&
			cacheStrategy == CACHE_STRATEGY && cacheSize == CACHE_SIZE);
	}
	
	@Test
	public void testPutGet() {
		String key = "foo2";
		String value = "bar2";
		String response = null;
		Exception ex = null;

		try {
			kvServer.putKV(key, value);
			response = kvServer.getKV(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.equals(value));
	}
	
	@Test
	public void testClearStorage() {
		String key = "foo2";
		String value = "bar2";
		String response = null;
		Exception ex = null;

		try {
			kvServer.putKV(key, value);
			kvServer.clearStorage();
			response = kvServer.getKV(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof FileNotFoundException && response == null);
	}
}
