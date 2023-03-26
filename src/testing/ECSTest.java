package testing;


import app_kvServer.KVServer;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ECSTest extends TestCase {
    @Test
    public void testAddingTwoServerToHashring() {

        Exception ex = null;
        KVServer server2;

        try {
            ConcurrentHashMap<BigInteger, IECSNode> hashRing = AllTests.ecsClient.getHashRing();
            assert hashRing.size() == 1;

            server2 = new KVServer(7001);
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");

            assert hashRing.size() == 2;
            System.out.println("SUCCESS: ECS hashring successfully registered 2 added servers.");

            server2.kill();

        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }
    @Test
    public void testAddingTwoServerToMetadata() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            ConcurrentHashMap<String, List<BigInteger>> metadata = AllTests.ecsClient.getMetadata();
            assert metadata.containsKey("localhost:7001");
            assert metadata.containsKey("localhost:7000");
            System.out.println("SUCCESS: ECS metadata successfully registered 2 added servers.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        assertNull(ex);
    }
    @Test
    public void testRemoveKilledServerFromMetadata() {
        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            ConcurrentHashMap<String, List<BigInteger>> metadata = AllTests.ecsClient.getMetadata();
            assert metadata.containsKey("localhost:7001");
            server2.kill();
            assert !metadata.containsKey("localhost:7001");
            System.out.println("SUCCESS: server removed from metadata after dying.");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }
    @Test
    public void testAddingTwoServerToMetadataRead() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            ConcurrentHashMap<String, List<BigInteger>> metadataRead = AllTests.ecsClient.getMetadataRead();
            assert metadataRead.containsKey("localhost:7001");
            assert metadataRead.containsKey("localhost:7000");
            System.out.println("SUCCESS: ECS metadataRead successfully registered 2 added servers.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        assertNull(ex);
    }
    @Test
    public void testRemoveKilledServerFromMetadataRead() {
        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            ConcurrentHashMap<String, List<BigInteger>> metadataRead = AllTests.ecsClient.getMetadataRead();
            assert metadataRead.containsKey("localhost:7001");
            server2.kill();
            assert !metadataRead.containsKey("localhost:7001");
            System.out.println("SUCCESS: server removed from metadataRead after dying.");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }


}
