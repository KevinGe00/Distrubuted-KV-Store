package performance;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class PerformanceTester{
    private static int testSize = 1000;
    public static int numRequests = 5000;
    public static KVServer server_KV;

    public static void m1(){
        try {
            System.out.println("Total number of put/get commands is " + testSize);
            ECSClient ecsClient = new ECSClient("localhost", 33333);
            ecsClient.initializeServer();
            ecsClient.run();

            server_KV = new KVServer(50000);
            server_KV.setHostname("localhost");
            server_KV.setBootstrapServer("localhost:33333");
            server_KV.initializeStore("out/50000");
            server_KV.start();

            for (int percentage = 20; percentage < 101; percentage = percentage + 20) {
                server_KV.clearStorage();

                for (int i = 1; i <= testSize; i++) {
                    server_KV.putKV("Key" + i, "Value" + i);
                }

                long start = System.currentTimeMillis();

                // Puts 200,400,600,or 800 random values
                for (int i = 1; i <= testSize * ((float) percentage / (float) testSize); i++) {
                    int randint = new Random().nextInt(testSize - 1) + 1;
                    server_KV.putKV("Key" + randint, "Value" + randint);
                }

                for (int i = 1; i <= testSize * (1 - ((float) percentage / (float) testSize)); i++) {
                    int randint = new Random().nextInt(testSize - 1) + 1;
                    String value = server_KV.getKV("Key" + randint);
                }

                long end = System.currentTimeMillis();

                System.out.println("Put percentage: " + percentage + "%, Average Latency: " + (end - start) + "ms");
                server_KV.close();
            }
        } catch (Exception e) {
            System.out.println("M1 Performance testing failed " + e);
        }
    }

    public static double m2_latency_throughput_test(Integer numServers, Integer numClients){
        try {

            // Create an ArrayList to store client threads and latencies
            KVServer[] servers = new KVServer[numServers];
            ArrayList<Thread> clientThreads = new ArrayList<>();
            final ArrayList<Long> clientLatencies = new ArrayList<>();

            long system_start = System.currentTimeMillis();
            System.out.println("Total number of put/get commands per client is " + numRequests);
            ECSClient ecsClient = new ECSClient("localhost", 33333);
            ecsClient.initializeServer();
            ecsClient.run();

            for (int i = 0; i < numServers; i++) {
                servers[i] = new KVServer(50000 + i + 1);
                servers[i].setHostname("localhost");
                servers[i].setBootstrapServer("localhost:33333");
                servers[i].initializeStore("out/" + Integer.toString(50000 + i + 1));
                new Thread(servers[i]).start();
            }

            for (int i = 1; i <= numClients; i++) {
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try {
                            KVClient client_KV = new KVClient();
                            client_KV.handleCommand("connect localhost 50001");

                            Random rand = new Random();
                            long totalLatency = 0;

                            for (int j = 1; j <= numRequests; j++) {
                                // Generate a random key-value pair
                                String key = Integer.toString(rand.nextInt(100));
                                String value = "value" + Integer.toString(rand.nextInt(100));

                                // Perform a random PUT or GET request
                                long startTime = System.currentTimeMillis();
                                if (rand.nextBoolean()) {
                                    client_KV.handleCommand("put " + key + " " + value);
                                } else {
                                    client_KV.handleCommand("get " + key);
                                }
                                long endTime = System.currentTimeMillis();
                                totalLatency += endTime - startTime;
                            }

                            // Add the total latency of this client to the list
                            clientLatencies.add(totalLatency);
                            client_KV.handleCommand("quit");

                        } catch (Exception e) {
                            e.printStackTrace();
                        }}});
                t.start();
                clientThreads.add(t);
                Thread.sleep(1000);
            }

            // Wait for all client threads to finish
            for (Thread t : clientThreads) {
                t.join();
            }

            // Compute and print the average latency across all clients
            long totalLatency = 0;
            for (long latency : clientLatencies) {
                totalLatency += latency;
            }
            double avgLatency = (double) totalLatency / (numClients * numRequests);

            // Clear the client latencies list for the next iteration
            clientLatencies.clear();

            for (KVServer server : servers) {
                server.close();
            }
            Thread.sleep(1000);
            return avgLatency;

        } catch (Exception e) {
            System.out.println("M2 Latency and Throughput testing failed " + e);
            return -1.0;
        }
    }

    public static double m3_add_remove_servers_time_test(Integer numServers){
        try {

            // Create an ArrayList to store client threads and latencies
            KVServer[] servers = new KVServer[numServers];

            long system_start = System.currentTimeMillis();
            ECSClient ecsClient = new ECSClient("localhost", 33333);
            new LogSetup("logs/performance/m3.log", Level.toLevel("ERROR"));
            ecsClient.initializeServer();
            ecsClient.run();

            servers[0] = new KVServer(50001);
            servers[0].setHostname("localhost");
            servers[0].setBootstrapServer("localhost:33333");
            servers[0].initializeStore("out" + File.separator + servers[0].getPort() + File.separator + "Coordinator");
            new Thread(servers[0]).start();

            // Add 100 values to first server
           Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        KVClient client_KV = new KVClient();
                        client_KV.handleCommand("connect localhost 50001");

                        for (int j = 0; j <= 5; j++) {
                            String key = Integer.toString(j);
                            String value = Integer.toString(j);

                            client_KV.handleCommand("put " + key + " " + value);
                        }

                        client_KV.handleCommand("quit");

                    } catch (Exception e) {
                        e.printStackTrace();
                    }}});
            t.start();
            Thread.sleep(1000);

            t.join();

            for (int i = 1; i < numServers-1; i++) {
                servers[i] = new KVServer(50000 + i + 1);
                servers[i].setHostname("localhost");
                servers[i].setBootstrapServer("localhost:33333");
                servers[i].initializeStore("out" + File.separator + servers[i].getPort() + File.separator + "Coordinator");
                new LogSetup("logs/performance/m3.log", Level.toLevel("ERROR"));
                new Thread(servers[i]).start();
            }

            long system_end = System.currentTimeMillis();
            double total_time = system_end - system_start;

            for (KVServer server : servers) {
                server.close();
            }
            Thread.sleep(1000);
            return total_time;

        } catch (Exception e) {
            System.out.println("M3 adding servers testing failed " + e);
            return -1.0;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        PerformanceTester performance = new PerformanceTester();
        new LogSetup("logs/performance/m3.log", Level.toLevel("ERROR"));
        Integer numServers = 3;
        Integer numClients = 20;

        //double avgLatency = performance.m2_latency_throughput_test(numServers,numClients);
        //Thread.sleep(1000);

        //System.out.println("#############################################################################################################################################################");
        //System.out.println("#################### Average latency across all clients for " + numServers + " servers and " + numClients + " clients: " + avgLatency + " ms. #################################");
        //System.out.println("#############################################################################################################################################################");

        double total_time = m3_add_remove_servers_time_test(20);
        System.out.println("Setting up " + numServers + " takes " + total_time + " ms.");
        System.exit(1);

    }
}

