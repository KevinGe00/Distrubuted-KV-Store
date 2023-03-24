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
    public void testSingleServerPredecessors() {

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


}
