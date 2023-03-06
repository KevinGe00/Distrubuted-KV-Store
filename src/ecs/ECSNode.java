package ecs;

import app_kvServer.KVServer;

public class ECSNode implements IECSNode{
    public enum Status {
        RUNNING,
        STOPPED, // Process still running but receiving client requests
        SHUTDOWN,
        TRANSFER
    }
    private String name;
    private String host;
    private int port;
    private String storeDir;
    private Status status;
    public ECSNode(String name, String host, int port, String storeDir) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.storeDir = storeDir;
        this.status = Status.STOPPED;
    }

    @Override
    public String getNodeName() {
        return this.name;
    }

    @Override
    public String getNodeHost() {
        return this.host;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[0];
    }
    public String getStoreDir() {
        return storeDir;
    }

}
