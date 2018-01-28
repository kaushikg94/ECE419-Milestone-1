package testing;

import java.io.FileNotFoundException;

import org.junit.Test;

import app_kvServer.KVServer;
import junit.framework.TestCase;
import app_kvServer.IKVServer.CacheStrategy;

public class ServerCacheTest extends TestCase {

	private KVServer lruServer;
	private KVServer lfuServer;
	private KVServer fifoServer;
	
	public void setUp() {
		lruServer = new KVServer(4000, 2, CacheStrategy.LRU);
		lfuServer = new KVServer(4001, 2, CacheStrategy.LFU);
		fifoServer = new KVServer(4002, 2, CacheStrategy.FIFO);
		lruServer.clearStorage();
	}

	public void tearDown() {
		lruServer.clearStorage();
		lruServer.close();
		lfuServer.close();
		fifoServer.close();
	}
	
	@Test
	public void testLruCache() {
		boolean inCache1 = false, inCache2 = false;
		Exception ex = null;

		try {
			lruServer.putKV("key1", "value");
			lruServer.putKV("key2", "value");
			lruServer.getKV("key1");
			inCache1 = lruServer.inCache("key2");
			lruServer.putKV("key3", "value");
			inCache2 = lruServer.inCache("key2");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && inCache1 == true && inCache2 == false);
	}
	
	@Test
	public void testLfuCache() {
		boolean inCache1 = false, inCache2 = false;
		Exception ex = null;

		try {
			lfuServer.putKV("key1", "value");
			lfuServer.putKV("key2", "value");
			lfuServer.getKV("key1");
			lfuServer.getKV("key2");
			lfuServer.getKV("key2");
			inCache1 = lfuServer.inCache("key1");
			lfuServer.putKV("key3", "value");
			inCache2 = lfuServer.inCache("key1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && inCache1 == true && inCache2 == false);
	}
	
	@Test
	public void testFifoCache() {
		boolean inCache1 = false, inCache2 = false;
		Exception ex = null;

		try {
			fifoServer.putKV("key1", "value");
			fifoServer.putKV("key2", "value");
			fifoServer.getKV("key1");
			inCache1 = fifoServer.inCache("key1");
			fifoServer.putKV("key3", "value");
			inCache2 = fifoServer.inCache("key1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && inCache1 == true && inCache2 == false);
	}
}
