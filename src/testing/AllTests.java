package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer kvServer = new KVServer(50000, 10, CacheStrategy.FIFO);
			kvServer.clearStorage();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		//clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		//clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(ServerTest.class);
		clientSuite.addTestSuite(ServerCacheTest.class);
		return clientSuite;
	}
}
