package app_kvECS;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageInterface;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import shared.messages.KVMessageInterface.StatusType;

/**
 * A runnable class responsible to communicate with servers,
 * also process server's requests.
 */
public class ECSClientChild implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    private Socket responseSocket;
    private String responseIP;
    private String responseHost;
    private int responsePort;
    private ECSClient ptrECSClient;
    private int ecsClientPort;
    // note: DataInput/OutputStream does not need to be closed.
    private DataInputStream input;
    private DataOutputStream output;

    public ECSClientChild(Socket responseSocket, ECSClient ptrECSClient) {
        this.responseSocket = responseSocket;
        responseIP = responseSocket.getInetAddress().getHostAddress();
        responseHost = responseSocket.getInetAddress().getHostName();
        responsePort = responseSocket.getPort();
        this.ptrECSClient = ptrECSClient;
        ecsClientPort = ptrECSClient.getPort();
    }

    @Override
    public void run() {
        try {
            input = new DataInputStream(new BufferedInputStream(responseSocket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(responseSocket.getOutputStream()));
        } catch (Exception e) {
            logger.error("Failed to create data stream for child socket of server #"
                    + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                    + responsePort, e);
            close();
        }

        while (true) {
            // === receive message ===
            KVMessage kvMsgRecv = null;
            try {
                // block until receiving response, exception when socket closed
                kvMsgRecv = receiveKVMessage();
            } catch (Exception e) {
                logger.error("Exception when receiving message at child socket of server #"
                        + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                        + responsePort
                        + " Not an error if caused by manual interrupt.", e);
                close();
                return;
            }

            // === process received message, and reply ===
            KVMessageInterface.StatusType statusRecv = kvMsgRecv.getStatus();
            String keyRecv = kvMsgRecv.getKey();
            String valueRecv = kvMsgRecv.getValue();

            if (statusRecv == StatusType.S2E_INIT_REQUEST_WITH_DIR) {
                // After a new server has established a connection with the ECS we need to
                // 1. Determines the position of the new storage server
                // 2. Recalculate and update the meta-data of the storage service
                // 3. Send the new storage server with the updated meta-data
                // 4. Set write lock on successor node and invoke data transfer

                if (ptrECSClient.addNewNode(responseHost, responsePort, valueRecv)) {
                    // Sanity check
                    String mdString = ptrECSClient.getMetadata().toString();
                    logger.info("Updated ECS copy of metadata: " + mdString);

                    // construct response
                    KVMessage kvMsgSend = new KVMessage();
                    kvMsgSend.setStatus(StatusType.E2S_INIT_RESPONSE_WITH_META);
                    kvMsgSend.setValue(mdString);

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }

                    // server received metadata, tell it to go from stopped to start
                    kvMsgSend = new KVMessage();
                    kvMsgSend.setStatus(StatusType.E2S_COMMAND_SERVER_RUN);

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }

                    // deal with successor node
                    String newNodeFullAddr = responseHost + ":" + responsePort;
                    String successor = ptrECSClient.successors.get(newNodeFullAddr);
                    ECSClientChild successorNodeRunnable = ptrECSClient.childObjects.get(successor).ecsClientChild;

                    // Sets a write lock on the successor node and invoke transfer of data items
                    KVMessage successorMsgSend = new KVMessage();
                    successorMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                    // value in the format of: "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port"
                    String val = valueRecv + ","
                            + ptrECSClient.getMetadata().get(newNodeFullAddr).get(0)
                            + "," + ptrECSClient.getMetadata().get(newNodeFullAddr).get(0)
                            + "," + responsePort;
                    successorMsgSend.setValue(val);

                    successorNodeRunnable.sendKVMessage(successorMsgSend);
                    // Wait for the successor to send back a notification to it's own ECS child that file transfer is complete

                } else {
                    logger.error("Failed to add server to ECS " + "IP: '"+ responseIP + "' \t port: "
                            + responsePort);
                }
            } else if (statusRecv == StatusType.S2E_SHUTDOWN_REQUEST) {
                // After we get message that a server is shutting down
                // 1. Remove corresponding node from hashring
                // 2. Recalculate and update the meta-data
                // 3. Set the write-lock on the server that is to be removed
                // 4. Send a meta-data update to the successor node

                ECSClient.RemovedNode removedNode =  ptrECSClient.removeNode(responseHost, responsePort);

                if (removedNode.success) {
                    KVMessage kvMsgSend = new KVMessage();
                    kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK);

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }

                    // update successor node meta data
                    ECSClientChild successorNodeRunnable = ptrECSClient.childObjects.get(removedNode.successor).ecsClientChild;
                    KVMessage successorMsgSend = new KVMessage();
                    String mdString = ptrECSClient.getMetadata().toString();

                    successorMsgSend.setStatus(StatusType.E2S_UPDATE_META_AND_RUN);
                    successorMsgSend.setValue(mdString);
                    successorNodeRunnable.sendKVMessage(successorMsgSend);

                    // Invoke data transfer from removed node to successor node
                    kvMsgSend = new KVMessage();
                    String successorStoreDir = ptrECSClient.getHashRing().get(removedNode.successor).getStoreDir();
                    kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                    // value in the format of: "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port"
                    String val = successorStoreDir + "," + removedNode.range.get(0).toString() + "," + removedNode.range.get(1).toString() + "," + removedNode.port;
                    kvMsgSend.setValue(val);

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }
                } else {
                    logger.error("Failed to remove server from ECS " + "IP: '"+ responseIP + "' \t port: "
                            + responsePort);
                }
            } else if (statusRecv == StatusType.S2E_SHUTDOWN_REQUEST) {
                for (Map.Entry<String, ECSClient.ChildObject> entry : ptrECSClient.childObjects.entrySet()) {
                    ECSClient.ChildObject value = entry.getValue();
                    ECSClientChild serverNodeRunnable = value.ecsClientChild;
                    String mdString = ptrECSClient.getMetadata().toString();

                    KVMessage nodeMsgSend = new KVMessage();
                    nodeMsgSend.setStatus(StatusType.E2S_UPDATE_META_AND_RUN);
                    nodeMsgSend.setValue(mdString);

                    serverNodeRunnable.sendKVMessage(nodeMsgSend);
                }
            }
        }
    }

    /**
     * Universal method to SEND a KVMessage via socket; will log the message.
     * Does NOT throw an exception.
     * @param kvMsg a KVMessage object
     * @return true for success, false otherwise
     */
    public boolean sendKVMessage(KVMessage kvMsg) {
        try {
            kvMsg.logMessageContent();
            byte[] bytes_msg = kvMsg.toBytes();
            // LV structure: length, value
            output.writeInt(bytes_msg.length);
            output.write(bytes_msg);
            logger.debug("#" + ecsClientPort + "-" + responsePort + ": "
                    + "Sending 4(length int) " +  bytes_msg.length + " bytes.");
            output.flush();
        } catch (Exception e) {
            logger.error("Exception when server #" + ecsClientPort + " sends to IP: '"
                    + responseIP + "' \t port: " + responsePort
                    + "\n Should close socket to unblock client's receive().", e);
            return false;
        }
        return true;
    }

    /**
     * Universal method to RECEIVE a KVMessage via socket; will log the message.
     * @return a KVMessage object
     * @throws Exception throws exception as closing socket causes receive() to unblock.
     */
    private KVMessage receiveKVMessage() throws Exception {
        KVMessage kvMsg = new KVMessage();
        // LV structure: length, value
        int size_bytes = input.readInt();
        logger.debug("#" + ecsClientPort + "-" + responsePort + ": "
                + "Receiving 4(length int) + " + size_bytes + " bytes.");
        byte[] bytes = new byte[size_bytes];
        input.readFully(bytes);
        if (!kvMsg.fromBytes(bytes)) {
            throw new Exception("#" + ecsClientPort + "-" + responsePort + ": "
                    + "Cannot convert all received bytes to KVMessage.");
        }
        kvMsg.logMessageContent();
        return kvMsg;
    }

    /**
     * Close response socket and change it to null.
     */
    private void close() {
        if (responseSocket == null) {
            return;
        }
        if (responseSocket.isClosed()) {
            responseSocket = null;
            return;
        }
        try {
            logger.debug("Child of server #" + ecsClientPort + " closing response socket"
                    + " connected to IP: '" + responseIP + "' \t port: "
                    + responsePort + ". \nExiting very soon.");
            responseSocket.close();
            responseSocket = null;
        } catch (Exception e) {
            logger.error("Unexpected Exception! Unable to close child socket of "
                    + "server #" + ecsClientPort + " connected to IP: '" + responseIP
                    + "' \t port: " + responsePort + "\nExiting very soon.", e);
            // unsolvable error, thread must be shut down now
            responseSocket = null;
        }
    }
}
