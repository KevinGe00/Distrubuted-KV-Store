package testing;

import java.io.File;
import java.io.IOException;

import app_kvECS.ECSClient;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.junit.AfterClass;


public class AllTests {
	public static KVServer server;
	public static ECSClient ecsClient;
	private static Logger logger = Logger.getRootLogger();


	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);

			ecsClient = new ECSClient("localhost", 30000);
			ecsClient.initializeServer();
			ecsClient.run();

			server = new KVServer(7000);
			server.setHostname("localhost");
			server.setBootstrapServer("localhost:30000");
			server.initializeStore("out" + File.separator + server.getPort() + File.separator + "Coordinator");
			new Thread(server).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ECSTest.class);
		clientSuite.addTestSuite(HashMapTest.class);
		clientSuite.addTestSuite(ReplicationTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(StoreTest.class);
//		clientSuite.addTestSuite(CLITest.class);
//		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}

	@AfterClass
	public static void tearDown() {
		server.kill();
		ecsClient.closeServerSocket();
	}
}
