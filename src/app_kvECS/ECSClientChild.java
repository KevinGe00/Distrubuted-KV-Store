package app_kvECS;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;

import app_kvClient.KVClient;
import app_kvECS.ECSClient.Mailbox;
import shared.messages.KVMessage;
import shared.messages.KVMessageInterface;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import shared.messages.KVMessageInterface.StatusType;


public class ECSClientChild implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    private String PROMPT;
    // server's
    private Socket responseSocket;
    private String responseIP;
    private int responsePort;
    private int serverListeningPort;
    private String storeDir;
    private String thisFullAddress;
    // note: DataInput/OutputStream does not need to be closed.
    private DataInputStream input;
    private DataOutputStream output;
    // ECS'
    private ECSClient ptrECSClient;
    private int ecsPort;
    // cache of ECS metadata, when different from ECS metadata, update it and send it to server
    String cacheMetadata;

    public ECSClientChild(Socket responseSocket, ECSClient ptrECSClient) {
        this.responseSocket = responseSocket;
        responseIP = responseSocket.getInetAddress().getHostAddress();
        responsePort = responseSocket.getPort();
        this.ptrECSClient = ptrECSClient;
        ecsPort = ptrECSClient.getPort();
        cacheMetadata = convertMetaHashmapToString(ptrECSClient.getMetadata()) + "|"
                        + convertMetaHashmapToString(ptrECSClient.getMetadataRead());
        PROMPT = "[To #<NOT INITIALIZED YET>] >>> ";
    }


    // convert metadata hashmap to form: range_from,range_to,ip,port;...;range_from,range_to,ip,port
    private String convertMetaHashmapToString(ConcurrentHashMap<String, List<BigInteger>> metaHashmap) {
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
    private ConcurrentHashMap<String, List<BigInteger>> convertStringToMetaHashmap(String str) {
        ConcurrentHashMap<String, List<BigInteger>> metaHashmap = new ConcurrentHashMap<>();
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


    /**
     * Run in ECS' child client(server) thread. For communication and processing.
     */
    @Override
    public void run() {
        try {
            input = new DataInputStream(new BufferedInputStream(responseSocket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(responseSocket.getOutputStream()));
        } catch (Exception e) {
            logger.error("Failed to create data stream for ECS #"
                    + ecsPort+ " connected to IP: '"+ responseIP + "' \t R-port: "
                    + responsePort, e);
            close();
            return;
        }

        // After a new server has established a connection with the ECS we need to
        // 1. Determines the position of the new storage server
        // 2. Recalculate and update the meta-data of the storage service
        // 3. Send the new storage server with the updated meta-data
        // 4. Set write lock on predecessor node and invoke data transfer

        // expect: init request with dir
        logger.info("Started initialization communication between ECS #" + ecsPort
                    + " and the server responsing at IP: '" + responseIP + "' \t R-port: " 
                    + responsePort + ".");
        KVMessage kvMsgRecv = null;
		StatusType statusRecv = null;
		String keyRecv = null;
		String valueRecv = null;
        try {
            // block until receiving response, exception when socket closed
            kvMsgRecv = receiveKVMessage();
        } catch (Exception e) {
            logger.error("Exception when receiving initialization message at ECS #"
                    + ecsPort+ " connected to IP: '"+ responseIP + "' \t R-port: "
                    + responsePort
                    + ".", e);
            close();
            return;
        }
        statusRecv = kvMsgRecv.getStatus();
        keyRecv = kvMsgRecv.getKey();       // key is server port
        valueRecv = kvMsgRecv.getValue();   // value is dir store of the server
        if (statusRecv != StatusType.S2E_INIT_REQUEST_WITH_DIR) {
            logger.error("Did not receive initialization request for server at IP: '" 
                        + responseIP + "' \t R-port: " + responsePort
                        + ". Server node was not added.");
            close();
            return;
        }
        serverListeningPort = Integer.parseInt(keyRecv);
        PROMPT = "[To #" + serverListeningPort + "] >>> ";
        storeDir = valueRecv;
        // For ECS, add new node
        if (!ptrECSClient.addNewNode(responseIP, serverListeningPort, storeDir)) {
            logger.error("Failed to add new Node for server at IP: '" + responseIP
                        + "' \t L-port: " + serverListeningPort);
            close();
            return;
        }    
        cacheMetadata = convertMetaHashmapToString(ptrECSClient.getMetadata()) + "|"
                        + convertMetaHashmapToString(ptrECSClient.getMetadataRead());
        // response server's init request with response (+ metadata)
        KVMessage kvMsgSend = new KVMessage();
        kvMsgSend.setStatus(StatusType.E2S_INIT_RESPONSE_WITH_META);
        kvMsgSend.setValue(cacheMetadata);
        if (!sendKVMessage(kvMsgSend)) {    // response to server's init
            close();
            return;
        }

        // send a WriteLock mail to predecessor, only if the predecessor is not myself
        thisFullAddress = responseIP + ":" + serverListeningPort;
        String predecessorFullAddress = ptrECSClient.predecessors.get(thisFullAddress);
        logger.info("thisFullAddress = " + thisFullAddress);
        logger.info("predecessorFullAddress = " + predecessorFullAddress);
        if (!thisFullAddress.equals(predecessorFullAddress)) {
            // in case that this is the first node, do not send the mail
            Mailbox mailTopredecessor = new Mailbox();
            mailTopredecessor.needsToSendWriteLock = true;
            // "storeDir_this,RangeFrom_this,RangeTo_this,IP_this,L-port_this"
            mailTopredecessor.valueSend_forWriteLock = storeDir + ","
                + ptrECSClient.getMetadata().get(thisFullAddress).get(0).toString(16) + ","
                + ptrECSClient.getMetadata().get(thisFullAddress).get(1).toString(16) + ","
                + responseIP + "," + serverListeningPort;
            ptrECSClient.childMailboxs.put(predecessorFullAddress, mailTopredecessor);
        }

        /*
         * server received metadata, predecessor should be setting Write Lock
         * command the server to RUN
         */ 
        kvMsgSend = new KVMessage();
        kvMsgSend.setStatus(StatusType.E2S_COMMAND_SERVER_RUN);
        if (!sendKVMessage(kvMsgSend)) {
            close();
            return;
        }
        logger.info("Finished initialization process for server at IP: '" + responseIP
                    + "' \t L-port: " + serverListeningPort + ".");
        
        /* ========= Main Communication Loop ========= */
        boolean needsSleep = false;

        while (!responseSocket.isClosed()) {
            if (needsSleep) {
                try {
                    Thread.sleep(100);
                    needsSleep = false;
                } catch (Exception e) {
                    break;
                }
            }

            kvMsgSend = null;

            String latestMetadata = convertMetaHashmapToString(ptrECSClient.getMetadata()) + "|"
                                    + convertMetaHashmapToString(ptrECSClient.getMetadataRead());
            Mailbox mail = ptrECSClient.childMailboxs.get(thisFullAddress);
            if (mail.needsToSendWriteLock) {
                // check Write Lock request first, clear mail as well
                // note: metdata change is done after the completion of Write Lock
                ptrECSClient.putEmptyMailbox(thisFullAddress);
                // set up Write Lock message
                kvMsgSend = new KVMessage();
                kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
                kvMsgSend.setValue(mail.valueSend_forWriteLock);
            } else if (!latestMetadata.equals(cacheMetadata)) {
                // check metadata cache, if changed, send a message to update server
                cacheMetadata = latestMetadata;
                // set up Metadata Update message
                kvMsgSend = new KVMessage();
                kvMsgSend.setStatus(StatusType.E2S_UPDATE_META_AND_RUN);
                kvMsgSend.setValue(cacheMetadata);
            } else {
                // set up an empty message for server to respond
                kvMsgSend = emptyCheck();
            }

            // send the message
            if (!sendKVMessage(kvMsgSend)) {
                break;
            }

            // === receive message ===
            kvMsgRecv = null;
            try {
                // block until receiving response, exception when socket closed
                kvMsgRecv = receiveKVMessage();
            } catch (Exception e) {
                logger.error("Exception when receiving message at ECS #"
                        + ecsPort+ " connected to IP: '"+ responseIP + "' \t L-port: "
                        + serverListeningPort + ".", e);
                break;
            }
            statusRecv = kvMsgRecv.getStatus();
            keyRecv = kvMsgRecv.getKey();
            valueRecv = kvMsgRecv.getValue();

            // === process received message, and reply ===

            if (statusRecv == StatusType.S2A_FINISHED_FILE_TRANSFER) {
                /*
                 * Expect to receive this after the server complete Write Lock.
                 * After receiving this, in next iteration, a Metadate Update should be sent
                 */
                String strPort_ServerRecvFile = keyRecv;
                if (!strPort_ServerRecvFile.equals(serverListeningPort)) {
                    logger.info("strPort_ServerRecvFile = " + strPort_ServerRecvFile);
                    logger.info("serverListeningPort = " + serverListeningPort);
                    ptrECSClient.replicateNewServer(responseIP + ":" + strPort_ServerRecvFile);
                }
                continue;
            }
            
            if (statusRecv == StatusType.S2E_SHUTDOWN_REQUEST) {
                shutdownProcess();  // this should close the socket and exit
                break;
            }
        }

        // remove node finally
        ECSClient.RemovedNode removedNode =  ptrECSClient.removeNode(responseIP, serverListeningPort);
        if (!removedNode.success) {
            logger.error(PROMPT + "Fail to remove Node. Terminating this thread.");
        }
        logger.debug(PROMPT + "Completed full shutdown process. Exiting this Thread.");
        close();
        return;
    }

    private void shutdownProcess() {
        // After we get message that a server is shutting down (updated 03-15)
        // 1. Set the write-lock on the server that is to be removed
        // 2. Remove corresponding node from hashring
        // 3. Recalculate and update the meta-data
        // 4. Send a meta-data update to the predecessor node

        KVMessage kvMsgSend;
        KVMessage kvMsgRecv;
        // 1. (Set the write-lock on the server that is to be removed) Send Write Lock message
        logger.debug(PROMPT + "Received shutdown request, sending back WRITE LOCK...");
        kvMsgSend = new KVMessage();
        kvMsgSend.setStatus(StatusType.E2S_WRITE_LOCK_WITH_KEYRANGE);
        String predecessorFullAddress = ptrECSClient.predecessors.get(thisFullAddress);
        if (predecessorFullAddress == null) {
            logger.error(PROMPT + "predecessors.get() return null for "
                        + thisFullAddress + ". Terminating this thread.");
            close();
            return;
        }
        kvMsgSend.setKey(predecessorFullAddress.split(":")[1]);
        if (thisFullAddress.equals(predecessorFullAddress)) {
            // Special Case: last server is shutting down
            if (!sendKVMessage(kvMsgSend)) {
                close();
                return;
            }
            try {
                // expect the server to close the socket and throw an expected Exception
                logger.debug(PROMPT + "Sent WRITE LOCK, expect last server to close the socket.");
                receiveKVMessage();
            } catch (Exception e) {
                logger.debug(PROMPT + "Server has shut down. Exiting this thread.");
            } finally {
                close();
                return;
            }
        }
        // General Case: need to wait for file transfer
        // "storeDir_predecessor,RangeFrom_this,RangeTo_this,IP_predecessor,L-port_predecessor"
        String predecessorStoreDir = ptrECSClient.getHashRing().get(hash(predecessorFullAddress)).getStoreDir();
        String[] predecessorIP_port = predecessorFullAddress.split(":");
        String valueSend_forWriteLock = 
            predecessorStoreDir + ","
            + ptrECSClient.getMetadata().get(thisFullAddress).get(0).toString(16) + ","
            + ptrECSClient.getMetadata().get(thisFullAddress).get(1).toString(16) + ","
            + predecessorIP_port[0] + "," + predecessorIP_port[1];
        kvMsgSend.setValue(valueSend_forWriteLock);
        if (!sendKVMessage(kvMsgSend)) {
            close();
            return;
        }
        try {
            // block until receiving response, exception when socket closed
            logger.debug(PROMPT + "WRITE LOCK sent, waiting for its file transfer.");
            kvMsgRecv = receiveKVMessage();
            while (kvMsgRecv.getStatus() != StatusType.S2A_FINISHED_FILE_TRANSFER) {
                kvMsgRecv = receiveKVMessage();
            }
            logger.debug(PROMPT + "It completed file transfer, removing node.");
        } catch (Exception e) {
            logger.error(PROMPT + "Did not receive response for file transfer. Terminating this thread.", e);
        }
        return;

        // 2. Remove corresponding node from hashring
        // 3. Recalculate and update the meta-data
        // 4. Send a meta-data update to the predecessor node
        // --> moved to outside the loop
    }

    // hash string to MD5 bigint
    public BigInteger hash(String fullAddress) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(fullAddress.getBytes());
            byte[] digest = md.digest();
            return new BigInteger(1, digest);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            throw new RuntimeException(e);
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
            kvMsg.logMessageContent(false);
            byte[] bytes_msg = kvMsg.toBytes();
            // LV structure: length, value
            output.writeInt(bytes_msg.length);
            output.write(bytes_msg);
            output.flush();
        } catch (Exception e) {
            logger.error("Exception when ECS #" + ecsPort + " sends to IP: '"
                    + serverListeningPort + "' \t port: " + responsePort
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
        byte[] bytes = new byte[size_bytes];
        input.readFully(bytes);
        if (!kvMsg.fromBytes(bytes)) {
            throw new Exception("ECS #" + ecsPort + "-" + serverListeningPort + ": "
                    + "Cannot convert all received bytes to KVMessage.");
        }
        kvMsg.logMessageContent(true);
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
            responseSocket.close();
            logger.debug(PROMPT + "Closed the socket. Exiting this thread.");
        } catch (Exception e) {
            logger.error(PROMPT + "Failed to close the socket. Terminating this thread.", e);
            // unsolvable error, thread must be shut down now
        } finally {
            responseSocket = null;
        }
    }

    private KVMessage emptyCheck() {
		KVMessage kvMsg = new KVMessage();
		kvMsg.setStatus(StatusType.E2S_EMPTY_CHECK);
		return kvMsg;
	}
}
