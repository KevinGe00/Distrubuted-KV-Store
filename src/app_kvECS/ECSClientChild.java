package app_kvECS;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;

import app_kvECS.ECSClient.WLPackage;
import shared.messages.KVMessage;
import shared.messages.KVMessageInterface;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import shared.messages.KVMessageInterface.StatusType;


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
    // copy of keyrange metadata, when different from the parent's metadata
    String strMetadata;

    public ECSClientChild(Socket responseSocket, ECSClient ptrECSClient) {
        this.responseSocket = responseSocket;
        responseIP = responseSocket.getInetAddress().getHostAddress();
        responseHost = responseSocket.getInetAddress().getHostName();
        responsePort = responseSocket.getPort();
        this.ptrECSClient = ptrECSClient;
        ecsClientPort = ptrECSClient.getPort();
        strMetadata = convertMetaHashmapToString(ptrECSClient.getMetadata());
    }

    // convert metadata hashmap to form: range_from,range_to,ip,port;...;range_from,range_to,ip,port
    private String convertMetaHashmapToString(HashMap<String, List<BigInteger>> metaHashmap) {
        String value = "";
        for (Map.Entry<String, List<BigInteger>> entry : metaHashmap.entrySet()) {
            if (value != "") {
                value = value + ";";
            }
            String fullAddress = entry.getKey();
            List<BigInteger> bigIntFrom_bigIntTo = entry.getValue();
            value = value + convertPairToString(fullAddress, bigIntFrom_bigIntTo);
        }
        return value;
    }
    // convert a single pair of hashmap to form: range_from,range_to,ip,port
    private String convertPairToString(String fullAddress, List<BigInteger> bigIntFrom_bigIntTo) {
        String[] ip_port = fullAddress.split(":");
        String ip = ip_port[0];
        String port = ip_port[1];
        String range_from = bigIntFrom_bigIntTo.get(0).toString(16);
        String range_to = bigIntFrom_bigIntTo.get(1).toString(16);
        return range_from+","+range_to+","+ip+","+port;
    }
    // convert string: "range_from,range_to,ip,port;...;range_from,range_to,ip,port" to metadata hashmap
    private HashMap<String, List<BigInteger>> convertStringToMetaHashmap(String str) {
        HashMap<String, List<BigInteger>> metaHashmap = new HashMap<>();
        String[] subStrs = str.split(";");
        for (String subStr : subStrs) {
            String[] rFrom_rTo_ip_port = subStr.split(",");
            String ip = rFrom_rTo_ip_port[2];
            String port = rFrom_rTo_ip_port[3];
            BigInteger range_from = new BigInteger(rFrom_rTo_ip_port[0], 16);
            BigInteger range_to = new BigInteger(rFrom_rTo_ip_port[1], 16);
            String fullAddess = ip + ":" + port;
            List<BigInteger> bigIntFrom_bigIntTo = Arrays.asList(range_from, range_to);
            metaHashmap.put(fullAddess, bigIntFrom_bigIntTo);
        }
        return metaHashmap;
    }

    @Override
    public void run() {
        try {
            input = new DataInputStream(new BufferedInputStream(responseSocket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(responseSocket.getOutputStream()));
        } catch (Exception e) {
            logger.error("Failed to create data stream for ECS #"
                    + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                    + responsePort, e);
            close();
        }

        // expect: init request with dir
        KVMessage kvMsgRecv = null;
		StatusType statusRecv = null;
		String keyRecv = null;
		String valueRecv = null;
        try {
            // block until receiving response, exception when socket closed
            kvMsgRecv = receiveKVMessage();
        } catch (Exception e) {
            logger.error("Exception when receiving initialization message at ECS #"
                    + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                    + responsePort
                    + ".", e);
            close();
            return;
        }
        statusRecv = kvMsgRecv.getStatus();
        valueRecv = kvMsgRecv.getValue();
        if (statusRecv == StatusType.S2E_INIT_REQUEST_WITH_DIR) {
            // After a new server has established a connection with the ECS we need to
            // 1. Determines the position of the new storage server
            // 2. Recalculate and update the meta-data of the storage service
            // 3. Send the new storage server with the updated meta-data
            // 4. Set write lock on successor node and invoke data transfer

            if (ptrECSClient.addNewNode(responseHost, responsePort, valueRecv)) {
                // Sanity check
                String mdString = convertMetaHashmapToString(ptrECSClient.getMetadata());
                logger.info("Updated ECS copy of metadata: " + mdString);

                // construct response
                KVMessage kvMsgSend = new KVMessage();
                kvMsgSend.setStatus(StatusType.E2S_INIT_RESPONSE_WITH_META);
                kvMsgSend.setKey(Integer.toString(responsePort));
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
                    System.out.println(newNodeFullAddr);

                    String successor = ptrECSClient.successors.get(newNodeFullAddr);
                    System.out.println(successor);


                // indicate the successor to send WL message
                // Sets a write lock on the successor node and invoke transfer of data items
                KVMessage successorMsgSend = new KVMessage();
                successorMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                // value in the format of: "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port"
                String val = valueRecv + ","
                        + ptrECSClient.getMetadata().get(newNodeFullAddr).get(0)
                        + "," + ptrECSClient.getMetadata().get(newNodeFullAddr).get(1)
                        + ",localhost," + responsePort;
                successorMsgSend.setValue(val);

                WLPackage pck;
                pck = new WLPackage();
                pck.needsWL = true;
                pck.valueSend = val;
                ptrECSClient.wlPackages.put(successor, pck);
                // Wait for the successor to send back a notification to it's own ECS child that file transfer is complete

            } else {
                logger.error("Failed to add server to ECS " + "IP: '"+ responseIP + "' \t port: "
                        + responsePort);
                close();
                return;
            }
        } else {
            // first must be an init request
            logger.error("Not an init request.");
            close();
            return;
        }
        boolean skipSleep = false;

        while (true) {
            if (!skipSleep) {
                try {
                Thread.sleep(500);
                } catch (Exception e) {}
            }
            skipSleep = false;

            // check your WLPack
            String fullAddress = responseHost + ":" + responsePort;
            WLPackage pck = ptrECSClient.wlPackages.get(fullAddress);
            // if true, send a WL to your own server
            if (pck.needsWL) {
                KVMessage msg = new KVMessage();
                msg.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                msg.setValue(pck.valueSend);
                if (!sendKVMessage(msg)) {
                    close();
                    return;
                }
                skipSleep = true;
            } else if (strMetadata != convertMetaHashmapToString(ptrECSClient.getMetadata())) {
                // if metadata has been updated, send update.
                strMetadata = convertMetaHashmapToString(ptrECSClient.getMetadata());
                KVMessage kvMsg = new KVMessage();
                kvMsg.setStatus(StatusType.E2S_UPDATE_META_AND_RUN);
                kvMsg.setKey(Integer.toString(responsePort));
                kvMsg.setValue(strMetadata);
                if (!sendKVMessage(emptyCheck())) {
                    close();
                    return;
                }
            } else {
                if (!sendKVMessage(emptyCheck())) {
                    close();
                    return;
                }
            }

            // === receive message ===
            kvMsgRecv = null;
            try {
                // block until receiving response, exception when socket closed
                kvMsgRecv = receiveKVMessage();
            } catch (Exception e) {
                logger.error("Exception when receiving message at ECS #"
                        + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                        + responsePort
                        + ".", e);
                close();
                return;
            }

            // === process received message, and reply ===
            statusRecv = kvMsgRecv.getStatus();
            keyRecv = kvMsgRecv.getKey();
            valueRecv = kvMsgRecv.getValue();

            if (statusRecv == StatusType.S2A_FINISHED_FILE_TRANSFER) {
                skipSleep = false;
                continue;
            }
            
            if (statusRecv == StatusType.S2E_SHUTDOWN_REQUEST) {
                // After we get message that a server is shutting down
                // 1. Remove corresponding node from hashring
                // 2. Recalculate and update the meta-data
                // 3. Set the write-lock on the server that is to be removed
                // 4. Send a meta-data update to the successor node

                ECSClient.RemovedNode removedNode =  ptrECSClient.removeNode(responseHost, responsePort);

                if (removedNode.success) {
                    KVMessage kvMsgSend = new KVMessage();
                    kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                    // set the value with the "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port"
                    // indicating sending all of its key range to new dir
                    // IP, port belongs to the server that receives the file
                    // the shuttingdown server will be responsible to send ECS and the server that receive the file a finish file transfer message
                    // once receiving that, ECS update metadata and broadcast it to all server for a metedata update.
                    // ^ implmented by each child thread keeps a copy of the metadata and compare to the common metedata in main ECS thread in each loop.
                    // ^ if a difference it detected, update its own copy and send updatemetadata to its own connected server

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }

                    // Invoke data transfer from removed node to successor node
                    kvMsgSend = new KVMessage();
                    String successorStoreDir = ptrECSClient.getHashRing().get(removedNode.successor).getStoreDir();
                    kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                    // value in the format of: "Directory_FileToHere,NewBigInt_from,NewBigInt_to,IP,port"
                    String val = successorStoreDir + ","
                                + removedNode.range.get(0).toString(16) + ","
                                + removedNode.range.get(1).toString(16) + ","
                                + "localhost," + removedNode.port;
                    kvMsgSend.setValue(val);

                    if (!sendKVMessage(kvMsgSend)) {
                        close();
                        return;
                    }

                    try {
                        // block until receiving response, exception when socket closed
                        kvMsgRecv = receiveKVMessage();
                    } catch (Exception e) {
                        logger.error("Exception when receiving message at ECS #"
                                + ecsClientPort+ " connected to IP: '"+ responseIP + "' \t port: "
                                + responsePort
                                + ".", e);
                        close();
                        return;
                    }

                    sendKVMessage(emptyCheck());
                    close();
                } else {
                    logger.error("Failed to remove server from ECS " + "IP: '"+ responseIP + "' \t port: "
                            + responsePort);
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
            logger.debug("ECS #" + ecsClientPort + "-" + responsePort + ": "
                    + "Sending 4(length int) " +  bytes_msg.length + " bytes.");
            output.flush();
        } catch (Exception e) {
            logger.error("Exception when ECS #" + ecsClientPort + " sends to IP: '"
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
        logger.debug("ECS #" + ecsClientPort + "-" + responsePort + ": "
                + "Receiving 4(length int) + " + size_bytes + " bytes.");
        byte[] bytes = new byte[size_bytes];
        input.readFully(bytes);
        if (!kvMsg.fromBytes(bytes)) {
            throw new Exception("ECS #" + ecsClientPort + "-" + responsePort + ": "
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
            logger.debug("ECS #" + ecsClientPort + " closing response socket"
                    + " connected to IP: '" + responseIP + "' \t port: "
                    + responsePort + ". \nExiting very soon.");
            responseSocket.close();
            responseSocket = null;
        } catch (Exception e) {
            logger.error("Unexpected Exception! Unable to ECS"
                    + "#" + ecsClientPort + " connected to IP: '" + responseIP
                    + "' \t port: " + responsePort + "\nExiting very soon.", e);
            // unsolvable error, thread must be shut down now
            responseSocket = null;
        }
    }

    private KVMessage emptyCheck() {
		KVMessage kvMsg = new KVMessage();
		kvMsg.setStatus(StatusType.E2S_EMPTY_CHECK);
		return kvMsg;
	}
}
