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
import shared.messages.KVMessage;
import shared.messages.KVMessageInterface;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
public class ReplicationTest extends TestCase {
    @Test
    public void testParentPath() {

        Exception ex = null;

        try {
            String store_path = AllTests.server.getDirStore();
            String actual_path = "out" + File.separator + AllTests.server.getPort();
            assert ECSClient.getParentPath(store_path) == actual_path;
            System.out.println("SUCCESS: store_path: " + store_path + ", actual parent store: " + actual_path);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }


    @Test
    public void testSingleServerNoReplicas() {
        Exception ex = null;
        try {
            String parent_path = ECSClient.getParentPath(AllTests.server.getDirStore());

            File folder = new File(parent_path);

            if (folder.isDirectory()) {
                File[] allFiles = folder.listFiles();
                ArrayList<File> subFolders = new ArrayList<>();

                for (File file : allFiles) {
                    if (file.isDirectory()) {
                        subFolders.add(file);
                    }
                }

                if (subFolders.size() == 1 && subFolders.get(0).getName().equals("Coordinator")) {
                    System.out.println("SUCCESS: Folder contains only one subfolder named Coordinator");
                    assert true;
                } else {
                    System.out.println("FAILURE: Folder does not meet the requirements");
                    assert false;
                }
            }

        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }
    @Test
    public void testTwoServerFolders() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            String server1_parent_path = ECSClient.getParentPath(AllTests.server.getDirStore());
            String server2_parent_path = ECSClient.getParentPath(server2.getDirStore());
            File directory1 = new File(server1_parent_path + File.separator + 7001);
            File directory2 = new File(server2_parent_path + File.separator + 7000);
            assert (directory1.exists() && directory1.isDirectory());
            assert (directory2.exists() && directory2.isDirectory());
            System.out.println("SUCCESS: Replicas created for two folders.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        assertNull(ex);
    }

    @Test
    public void testThreeServerFolders() {

        Exception ex = null;
        KVServer server2 = new KVServer(7001);
        KVServer server3 = new KVServer(7002);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");

            server3.setHostname("localhost");
            server3.setBootstrapServer("localhost:30000");
            server3.initializeStore("out" + File.separator + server3.getPort() + File.separator + "Coordinator");

            String server1_parent_path = ECSClient.getParentPath(AllTests.server.getDirStore());
            String server2_parent_path = ECSClient.getParentPath(server2.getDirStore());
            String server3_parent_path = ECSClient.getParentPath(server3.getDirStore());
            File directory12 = new File(server1_parent_path + File.separator + 7001);
            File directory13 = new File(server1_parent_path + File.separator + 7002);
            File directory21 = new File(server2_parent_path + File.separator + 7000);
            File directory23 = new File(server2_parent_path + File.separator + 7002);
            File directory31 = new File(server3_parent_path + File.separator + 7000);
            File directory32 = new File(server3_parent_path + File.separator + 7001);

            assert (directory12.exists() && directory12.isDirectory());
            assert (directory13.exists() && directory13.isDirectory());
            assert (directory21.exists() && directory21.isDirectory());
            assert (directory23.exists() && directory23.isDirectory());
            assert (directory31.exists() && directory31.isDirectory());
            assert (directory32.exists() && directory32.isDirectory());
            System.out.println("SUCCESS: Replicas created for three folders.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        server3.kill();
        assertNull(ex);
    }

    @Test
    public void testTwoReplicaFoldersWithData() {
        String key = "hello";
        String value = "world";
        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            AllTests.server.putKV(key, value);
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");

            File original = new File(AllTests.server.getDirStore() + File.separator + "hello");
            File replica = new File(ECSClient.getParentPath(server2.getDirStore()) + File.separator + AllTests.server.getPort() + File.separator + "hello");

            assert (original.exists() && replica.exists());
            System.out.println("SUCCESS: Replica contains data.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
        assertNull(ex);
    }

    @Test
    public void testWrongServerBeforeReplication() {
        String key = "hello";
        String value = "world";
        Exception ex = null;
        KVServer server2 = new KVServer(7001);

        try {
            server2.setHostname("localhost");
            server2.setBootstrapServer("localhost:30000");
            server2.initializeStore("out" + File.separator + server2.getPort() + File.separator + "Coordinator");
            server2.putKV(key, value);
            assert false;
            System.out.println("FAILURE: Wrong server stores put request before replication.");
        } catch (Exception e) {
            ex = e;
        }

        server2.kill();
    }

}
