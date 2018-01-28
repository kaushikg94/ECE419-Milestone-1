package app_kvServerPerfTest;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.io.IOException;
import java.net.BindException;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.CacheStrategy;
import cache.ICache;
import cached_storage.CachedStorage;

public class KVServerPerfTest {

	private static final double NS_TO_MS = 1000000;

    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0], cache size (# entries)
	 * at args[1], and cache strategy (one of: None, LRU, LFU, FIFO) at args[2]
     */
    public static void main(String[] args) {
    	try {
			// Check argument count
			if(args.length != 5) {
				System.out.println("Error: Invalid number of arguments");
				System.out.println("Usage: ServerPerfTest <cache size> " +
					"<cache strategy> <percent puts (100 = all puts)> " +
					"<n requests to test> <n unique keys to use>");
				return;
			}

			// Parse arguments
			int cacheSize = Integer.parseInt(args[0]);

			CacheStrategy cacheStrategy;
			switch(args[1]) {
				case "LRU":
					cacheStrategy = CacheStrategy.LRU;
					break;
				case "LFU":
					cacheStrategy = CacheStrategy.LFU;
					break;
				case "FIFO":
					cacheStrategy = CacheStrategy.FIFO;
					break;
				default:
					System.out.println("Error: Invalid cache strategy " +
						"(should be one of LRU, LFU, or FIFO)");
					return;
			}

			int percentGets = Integer.parseInt(args[2]);
			double getRequestsRatio = percentGets / 100.0;

			int nRequestsToTest = Integer.parseInt(args[3]);

			int nUniqueKeys = Integer.parseInt(args[4]);

			// Create server
			KVServer server = new KVServer(4000, cacheSize, cacheStrategy);

			// Repeat for number of tests specified and record results
			Random rand = new Random();
			long[] elapsedPerRequest = new long[nRequestsToTest];
			for(int i = 0; i < nRequestsToTest; i++) {
				// Select a type of request to make
				boolean doGet = rand.nextDouble() < getRequestsRatio;

				// Select a random key to use
				String key = "key" + rand.nextInt(nUniqueKeys);

				// Make the request, timing it
				long startTime = System.nanoTime();
				if(doGet) {
					try {
						server.getKV(key);
					} catch(Exception e) {
						// Ignore
					}
				} else {
					try {
						server.putKV(key, "value");
					} catch(Exception e) {
						// Ignore
					}
				}
				long endTime = System.nanoTime();

				// Compute and save elapsed time
				long elapsed = endTime - startTime;
				elapsedPerRequest[i] = elapsed;
			}

			// Compute statistics over all requests
			long totalElapsed = 0;
			long minElapsed = elapsedPerRequest[0];
			long maxElapsed = elapsedPerRequest[0];
			for(int i = 0; i < nRequestsToTest; i++) {
				totalElapsed += elapsedPerRequest[i];
				minElapsed = Math.min(minElapsed, elapsedPerRequest[i]);
				maxElapsed = Math.max(maxElapsed, elapsedPerRequest[i]);
			}
			long averageElapsed = totalElapsed / nRequestsToTest;
			System.out.println("Average time per request: " + (averageElapsed / NS_TO_MS) + " ms");
			System.out.println("Min time for request: " + (minElapsed / NS_TO_MS) + " ms");
			System.out.println("Max time for request: " + (maxElapsed / NS_TO_MS) + " ms");

		} catch (NumberFormatException e) {
			System.out.println("Error: Invalid number in arguments");
				System.out.println("Usage: ServerPerfTest <cache size> " +
					"<cache strategy> <percent puts (100 = all puts)> " +
					"<n requests to test> <n unique keys to use>");
			System.exit(1);
		}
    }
}
