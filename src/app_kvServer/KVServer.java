package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;
import org.apache.commons.cli.*;

public class KVServer extends Thread implements IKVServer {

	private int port;
	private String address;
	private String storeDirectory;
	private String logfilePath;
	private String logLevel;
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	private static Options buildOptions() {
		//	Setting up command line params
		Options options = new Options();

		Option port = new Option("p", "port", true, "Sets the port of the server");
		port.setRequired(true);
		port.setType(Integer.class);
		options.addOption(port);

		Option address = new Option("a", "address", true, "Which address the server should listen to, default set to localhost");
		address.setRequired(false);
		address.setType(String.class);
		options.addOption(address);

		Option directory = new Option("d", "directory", true, "Directory for files (Put here the files you need to persist the data, the directory is created upfront and you can rely on that it exists)");
		directory.setRequired(true);
		directory.setType(String.class);
		options.addOption(directory);

		Option logfilePath = new Option("l", "logfilePath", true, "Relative path of the logfile, e.g., “echo.log”, default set to be current directory\n");
		logfilePath.setRequired(false);
		logfilePath.setType(String.class);
		options.addOption(logfilePath);

		Option loglevel = new Option("ll", "loglevel", true, "Loglevel, e.g., INFO, ALL, …, default set to be ALL");
		loglevel.setRequired(false);
		loglevel.setType(String.class);
		options.addOption(loglevel);

		return options;
	}

	/**
	 * Main entry point for the KVstore server application.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			Options options = buildOptions();

			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine cmd;

			try {
				cmd = parser.parse(options, args);
			} catch (ParseException e) {
				System.out.println(e.getMessage());
				formatter.printHelp("command", options);
				return;
			}

			int portVal = Integer.parseInt(cmd.getOptionValue("p"));

			KVServer server = new KVServer(portVal, 0, "l");
			server.address = cmd.getOptionValue("a", "localhost");
			server.storeDirectory = cmd.getOptionValue("d");
			server.logfilePath = cmd.getOptionValue("l", ".");
			server.logLevel = cmd.getOptionValue("ll", "ALL");

			server.start();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
