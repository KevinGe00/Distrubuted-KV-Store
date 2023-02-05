package testing;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;

import java.util.Random;

public class PerformanceTester extends TestCase {
    private int testSize = 1000;
    public void test2080() {
        try {
            System.out.println("Total number of put/get commands is " + testSize);
            for (int percentage = 20; percentage < 100; percentage = percentage + 20) {

                KVServer server = new KVServer(33333, testSize, "None");
                server.clearStorage();

                for (int i = 1; i <= testSize; i++) {
                    server.putKV("Key" + i, "Value" + i);
                }

                long start = System.currentTimeMillis();

                // Puts 200,400,600,or 800 random values
                for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                    int randint = new Random().nextInt(testSize-1)+1;
                    server.putKV("Key" + randint, "Value" + randint);
                }

                for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                    int randint = new Random().nextInt(testSize-1)+1;
                    String value = server.getKV("Key" + randint);
                    assertTrue(value.equals(String.valueOf(randint)) || value.equals(null) );
                }

                long end = System.currentTimeMillis();

                System.out.println("Put percentage: " + percentage + "Total Processing time (divide by testSize for average latency): " + (end - start) + "ms");
                server.close();
            }
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
            System.out.println("Performance testing failed " + e);
        }
    }
}
