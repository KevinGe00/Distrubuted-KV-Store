package testing;


import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
public class HashMapTest extends TestCase {
    @Test
    public void testSingleServerSuccessor() {

        Exception ex = null;

        try {
            HashMap<String, String> successors = AllTests.ecsClient.getSuccessors();
            assert successors.get("localhost:7000") == "localhost:7000";
            System.out.println("SUCCESS: Single server is its own successor");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testSingleServerPredecessor() {

        Exception ex = null;

        try {
            HashMap<String, String> predecessors = AllTests.ecsClient.getPredecessors();
            assert predecessors.get("localhost:7000") == "localhost:7000";
            System.out.println("SUCCESS: Single server is its own predecessor");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    public void testDoubleServerSuccessor() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            HashMap<String, String> successors = AllTests.ecsClient.getSuccessors();
            assert successors.get("localhost:7000") == "localhost:7001";
            assert successors.get("localhost:7001") == "localhost:7000";
            System.out.println("SUCCESS: Double servers has the other as its successor.");
        } catch (Exception e) {
            ex = e;
        }
        server2.kill();
        assertNull(ex);
    }
    @Test
    public void testDoubleServerPredecessor() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            HashMap<String, String> predecessors = AllTests.ecsClient.getPredecessors();
            assert predecessors.get("localhost:7000") == "localhost:7001";
            assert predecessors.get("localhost:7001") == "localhost:7000";
            System.out.println("SUCCESS: Double servers has the other as its predecessor.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        assertNull(ex);
    }


}
