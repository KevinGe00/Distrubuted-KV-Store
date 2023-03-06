package testing;

import java.io.IOException;

import app_kvECS.ECSClient;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	public static KVServer server_KV;

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);

			ECSClient ecsClient = new ECSClient("localhost", 33333);
			ecsClient.initializeServer();
			ecsClient.run();

			server_KV = new KVServer(50000);
			server_KV.setHostname("localhost");
			boolean check = server_KV.setBootstrapServer("localhost:33333");
			server_KV.initializeStore("out/50000");
			new Thread(server_KV).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(StoreTest.class);
		clientSuite.addTestSuite(CLITest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}
	
}
