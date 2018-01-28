package storage;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystemException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer;

public class Storage implements IStorage {

	private static Logger logger = Logger.getRootLogger();

    private String rootDir;

    /**
	 * Constructs a new Storage object to manage persistent storage.
	 * @param rootDir the root directory to store all data in
	 */
    public Storage(String rootDir) {
        logger.info("Initializing persistent storage");
        this.rootDir = rootDir;
        new File(rootDir).mkdirs();
    }

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    @Override
    public boolean inStorage(String key) {
        // Check if a file with the specified key exists
        File file = new File(rootDir, getMd5Hash(key));
        return file.isFile();
    }

    /**
     * Get the value associated with the key, assume caller has already checked
     * that it exists before calling
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public String getKV(String key) throws Exception {
        logger.info("Getting from storage: " + key);
        File file = new File(rootDir, getMd5Hash(key));
        FileReader fr = new FileReader(file.getPath());
        BufferedReader br = new BufferedReader(fr);
        String value = br.readLine();
        br.close();
        fr.close();
        return value;
    }

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public void putKV(String key, String value) throws Exception {
        // Create file if it does not exist
        File file = new File(rootDir, getMd5Hash(key));
        if(!file.isFile()) {
            logger.info("Inserting into storage: " + key);
            if(!file.createNewFile()) {
                throw new FileSystemException("Unable to create key");
            }
        } else {
           logger.info("Updating in storage: " + key);
        }

        // Write data to file (overwriting any previous data)
        PrintWriter out = new PrintWriter(file);
        out.print(value);
        out.close();
    }

    /**
     * Delete the key-value pair from storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public void deleteKV(String key) throws Exception {
        if(!inStorage(key)) {
            throw new FileNotFoundException("Specified key not found");
        }
        logger.info("Deleting from storage: " + key);
        File file = new File(rootDir, getMd5Hash(key));
        if(!file.delete()) {
            throw new FileSystemException("Unable to delete key");
        }
    }

    /**
     * Clear the storage of the server
     */
    @Override
    public void clear() throws Exception {
        // Delete all files in root directory
        logger.info("Clearing storage");
        File rootDirectory = new File(rootDir);
        for(File file : rootDirectory.listFiles()) {
            if(!file.delete()) {
                throw new FileSystemException("Unable to delete key: " +
                    file.getName());
            }
        }
    }

    /**
     * Get the MD5 hash for a given string
     */
    private String getMd5Hash(String str) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(str.getBytes(), 0, str.length());
            return new BigInteger(1, digest.digest()).toString(16);
        } catch(NoSuchAlgorithmException e) {
            logger.fatal("Unable to use MD5 hashing");
            System.exit(1);
        }
        return "";
    }
}
