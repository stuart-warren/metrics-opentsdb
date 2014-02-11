package com.stuartwarren.metrics.opentsdb;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * A client to a tcollector.
 */
public class OpenTSDB implements Closeable {
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final int BUF_SIZE = UDPOutputStream.DEFAULT_BUFFER_SIZE;
    public static final String DEFAULT_ADDRESS = "127.0.0.1";
    public static final int DEFAULT_PORT = 8953;

    private OutputStream outputstream;
    private final Charset charset;

    private Writer writer;
    private int failures;
    
    /**
     * Creates a new client which connects to the default local address the
     * udp_bridge tcollector listens on.
     * @throws IOException 
     * @throws SocketException 
     */
    public OpenTSDB() {
        this(new InetSocketAddress(DEFAULT_ADDRESS, DEFAULT_PORT));
    }


    /**
     * Creates a new client which connects to the given address.
     *
     * @param address       the address of the Carbon server
     * @throws IOException 
     * @throws SocketException 
     */
    public OpenTSDB(InetSocketAddress address){
        this(address, UTF_8, BUF_SIZE);
    }

    /**
     * Creates a new client which connects to the given address using the given
     * character set.
     *
     * @param address       the address of the Carbon server
     * @param charset       the character set used by the server
     * @param bufferSize    the length of the buffer. Must be at least 1 byte long
     * @throws IOException 
     * @throws SocketException 
     */
    public OpenTSDB(InetSocketAddress address, Charset charset, int bufferSize){
        this.charset = charset;
        try {
            this.outputstream = new UDPOutputStream(address.getAddress(), address.getPort(), bufferSize);
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Creates a connection.
     *
     * @throws IllegalStateException if the client is already connected
     * @throws IOException           if there is an error connecting
     */
    public void connect(){
        if (writer != null) {
            throw new IllegalStateException("Already connected");
        }

        this.writer = new BufferedWriter(new OutputStreamWriter(this.outputstream, charset));
    }

    /**
     * Sends the given measurement to the server.
     *
     * @param name      the name of the metric
     * @param value     the value of the metric
     * @param timestamp the timestamp of the metric
     * @throws IOException if there was an error sending the metric
     */
    public void send(String name, String value, long timestamp) throws IOException {
        try {
            writer.write(sanitize(name));
            writer.write(' ');
            writer.write(Long.toString(timestamp));
            writer.write(' ');
            writer.write(sanitize(value));
            //TODO: add optional tags here
            writer.write('\n');
            writer.flush();
            this.failures = 0;
        } catch (IOException e) {
            failures++;
            throw e;
        }
    }

    /**
     * Returns the number of failed writes to the server.
     *
     * @return the number of failed writes to the server
     */
    public int getFailures() {
        return failures;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            this.writer.flush();
            this.outputstream.close();
            this.writer.close();
        }
        this.writer = null;
    }

    protected String sanitize(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }
}
