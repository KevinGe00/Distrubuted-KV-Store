package app_kvECS;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.lang.Integer;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import org.apache.log4j.*;

import shared.messages.KVMessage;
import shared.messages.KVMessageInterface.StatusType;
import shared.messages.KVMessageInterface;
import app_kvServer.IKVServer.SerStatus;

/**
 * Represents a connection end point for a particular server that is
 * connected to the ECS. This class is responsible for message reception
 * and sending.
 */


public class ServerECSConnection implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private Socket responseSocket;
    private String responseIP;
    private int responsePort;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1 + 1 + 20 + 3 + 120000;

    private Socket serverSocket;
    private ECSClient handleECS;
    private int ECSPort;
    // note: DataInput/OutputStream does not need to be closed.
    private DataInputStream input;
    private DataOutputStream output;

    /**
     * Constructs a new ServerECSConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ServerECSConnection(Socket responseSocket, ECSClient handleECS) {
        this.responseSocket = responseSocket;
        responseIP = responseSocket.getInetAddress().getHostAddress();
        responsePort = responseSocket.getPort();
        this.handleECS = handleECS;
        ECSPort = handleECS.getPort();
    }

    /**
     * Initializes and starts the server/ECS connection.
     */
    public void run() {
        try {
            input = new DataInputStream(new BufferedInputStream(responseSocket.getInputStream()));
            output = new DataOutputStream(new BufferedOutputStream(responseSocket.getOutputStream()));
        } catch (Exception e) {
            logger.error("Failed to create data stream for child socket of server #"
                    + ECSPort + " connected to IP: '"+ responseIP + "' \t port: "
                    + responsePort, e);
            close();
            return;
        }

        while (handleECS.getRunning()) {
            // === receive message ===
            KVMessage kvMsgRecv = null;
            try {
                // block until receiving response, exception when socket closed
                kvMsgRecv = receiveKVMessage();
            } catch (Exception e) {
                logger.error("Exception when receiving message at child socket of server #"
                        + ECSPort+ " connected to IP: '"+ responseIP + "' \t port: "
                        + responsePort
                        + " Not an error if caused by manual interrupt.", e);
                close();
                return;
            }
            // =======================

            // === process received message, and reply ===
            KVMessageInterface.StatusType statusRecv = kvMsgRecv.getStatus();
            String keyRecv = kvMsgRecv.getKey();
            String valueRecv = kvMsgRecv.getValue();

            // 2. normal KV response to server
            switch (statusRecv) {
                case ECS_SERVER_STOP_SUCCESS: {
                    // Remove server from metadata
                }
                case ECS_SERVER_TRANSFER_COMPLETE: {
                    // Send back message ECS_SERVER_RUN
                }
                default: {
                    logger.error("Invalid message <" + statusRecv.name()
                            + "> received at child socket of server #"
                            + ECSPort + " connected to IP: '"+ responseIP
                            + "' \t port: " + responsePort);
                    close();
                    return;
                }
            }
            // ===========================================
        }
        close();
        return;
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
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
            logger.debug("Child of ECS #" + ECSPort + " closing response socket"
                    + " connected to IP: '" + responseIP + "' \t port: "
                    + responsePort + ". \nExiting very soon.");
            responseSocket.close();
            responseSocket = null;
        } catch (Exception e) {
            logger.error("Unexpected Exception! Unable to close child socket of "
                    + "ECS #" + ECSPort + " connected to IP: '" + responseIP
                    + "' \t port: " + responsePort + "\nExiting very soon.", e);
            // unsolvable error, thread must be shut down now
            responseSocket = null;
        }
    }

    /**
     * Universal method to SEND a KVMessage via socket; will log the message.
     * Does NOT throw an exception.
     * @param kvMsg a KVMessage object
     * @return true for success, false otherwise
     */
    private boolean sendKVMessage(KVMessage kvMsg) {
        try {
            kvMsg.logMessageContent();
            byte[] bytes_msg = kvMsg.toBytes();
            // LV structure: length, value
            output.writeInt(bytes_msg.length);
            output.write(bytes_msg);
            logger.debug("#" + ECSPort + "-" + responsePort + ": "
                    + "Sending 4(length int) " +  bytes_msg.length + " bytes.");
            output.flush();
        } catch (Exception e) {
            logger.error("Exception when ECS #" + ECSPort + " sends to IP: '"
                    + responseIP + "' \t port: " + responsePort
                    + "\n Should close socket to unblock server's receive().", e);
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
        logger.debug("#" + ECSPort + "-" + responsePort + ": "
                + "Receiving 4(length int) + " + size_bytes + " bytes.");
        byte[] bytes = new byte[size_bytes];
        input.readFully(bytes);
        if (!kvMsg.fromBytes(bytes)) {
            throw new Exception("#" + ECSPort + "-" + responsePort + ": "
                    + "Cannot convert all received bytes to KVMessage.");
        }
        kvMsg.logMessageContent();
        return kvMsg;
    }
}
