package testing;
import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvECS.ECSClient;
import app_kvServer.IKVServer;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import org.junit.Before;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class ECSCLITest extends TestCase{

    private static KVServer server_KV;

    private static ECSClient ecsClient;

    @Before
    public void setUpECS() {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);

            ECSClient ecsClient = new ECSClient("localhost", 44444);
            ecsClient.initializeServer();
            ecsClient.run();

            server_KV = new KVServer(44445);
            server_KV.setHostname("localhost");
            server_KV.setBootstrapServer("localhost:44444");
            server_KV.initializeStore("out/44445");
            new Thread(server_KV).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testECSStartServer() {
        Exception ex = null;

        try {
            ecsClient.handleCommand("start");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue((ex == null) && (server_KV.getSerStatus() == IKVServer.SerStatus.RUNNING));
    }

    @Test
    public void testECSStopServer() {
        Exception ex = null;

        try {
            ecsClient.handleCommand("stop");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && server_KV.getSerStatus() == IKVServer.SerStatus.STOPPED);
    }

    @Test
    public void testECSKillServer() {
        Exception ex = null;

        try {
            ecsClient.handleCommand("kill");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && server_KV.getSerStatus() == IKVServer.SerStatus.SHUTTING_DOWN);
    }




}
