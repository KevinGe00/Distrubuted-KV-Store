package app_kvECS;

import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.lang.Integer;

import app_kvServer.KVServer;
import org.apache.log4j.*;

import app_kvServer.KVServer.Status;
import shared.messages.KVMessage;
import shared.messages.KVMessageObj;
import shared.messages.KVMessage.StatusType;
/**
 * Represents a connection end point for a particular server that is
 * connected to the ECS. This class is responsible for message reception
 * and sending.
 */


public class ServerECSConnection implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1 + 1 + 20 + 3 + 120000;

    private Socket serverSocket;
    private ECSClient handleECS;
    private InputStream input;
    private OutputStream output;

    /**
     * Constructs a new ServerECSConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ServerECSConnection(Socket serverSocket, ECSClient handleECS) {
        this.serverSocket = serverSocket;
        this.handleECS = handleECS;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the server/ECS connection.
     */
    public void run() {
        logger.info("Server to ECS connection established.");
        while (isOpen) {
            try {
                output = serverSocket.getOutputStream();
                input = serverSocket.getInputStream();

                KVMessageObj serverMsg = receiveMessage();

                switch (serverMsg.getStatus()) {
                    case GET:
                        try {
                        } catch (Exception e) {
                        }
                        break;
                    default:
                        break;
                }
            } catch (Exception ioe) {
                logger.error("Error! Connection could not be established!", ioe);
            } finally {
            }
        }
    }
    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */

    public void sendMessage(KVMessageObj msg) throws IOException {
        byte[] msgBytes = new byte[0];
        byte[] b1, b2;

        switch (msg.getStatus()) {
            case GET_SUCCESS:
                break;
            default:
                break;
        }

        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + serverSocket.getInetAddress().getHostAddress() + ":"
                + serverSocket.getPort() + ">: '"
                + msg.getStatus() +"'");
    }

    private KVMessageObj receiveMessage() throws IOException {
        KVMessageObj kvMsg = new KVMessageObj();

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first 1 char - StatusType from stream */
        int statusIdx = input.read();
        StatusType statusMsg = StatusType.values()[statusIdx];
        kvMsg.setStatus(statusMsg);

        /* read second 1 char - byte length of Key from stream*/
        int lenBytesKey = input.read();

        byte read = (byte) input.read();
        boolean reading = true;
        while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* get key */
        kvMsg.setKey(new String(Arrays.copyOfRange(msgBytes, 0, lenBytesKey)));

        if (statusMsg == StatusType.PUT) {
            byte b3 = msgBytes[lenBytesKey];
            byte b2 = msgBytes[lenBytesKey+1];
            byte b1 = msgBytes[lenBytesKey+2];
            int lenBytesValue = ((b3 & 0xff) << 16) | ((b2 & 0xff) << 8) | (b1 & 0xff);
            kvMsg.setValue(new String(Arrays.copyOfRange(msgBytes,
                    lenBytesKey+3,
                    lenBytesKey+3+lenBytesValue)));
            logger.info("RECEIVE \t<"
                    + serverSocket.getInetAddress().getHostAddress() + ":"
                    + serverSocket.getPort() + ">: '"
                    + kvMsg.getStatus() + "' "
                    + kvMsg.getKey() + " "
                    + kvMsg.getValue());
        } else {
            logger.info("RECEIVE \t<"
                    + serverSocket.getInetAddress().getHostAddress() + ":"
                    + serverSocket.getPort() + ">: '"
                    + kvMsg.getStatus() + "' "
                    + kvMsg.getKey());
        }
        return kvMsg;
    }
}
