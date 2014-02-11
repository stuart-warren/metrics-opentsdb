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
    private static final int DEFAULT_BUF_SIZE = UDPOutputStream.DEFAULT_BUFFER_SIZE;
    public static final String DEFAULT_ADDRESS = "127.0.0.1";
    public static final int DEFAULT_PORT = 8953;

    private final Charset charset;

    private OutputStream outputstream;
    private InetSocketAddress address;
    private int bufferSize;
    private Writer writer;
    private int failures;
    
    
    /**
     * Creates a new client which connects to the default local address the
     * udp_bridge tcollector listens on.
     */
    public OpenTSDB() {
        this(new InetSocketAddress(DEFAULT_ADDRESS, DEFAULT_PORT));
    }


    /**
     * Creates a new client which connects to the given address.
     *
     * @param address       the address of the Carbon server
     */
    public OpenTSDB(InetSocketAddress address){
        this(address, UTF_8, DEFAULT_BUF_SIZE);
    }

    /**
     * Creates a new client which connects to the given address using the given
     * character set.
     *
     * @param address       the address of the Carbon server
     * @param charset       the character set used by the server
     * @param bufferSize    the length of the buffer. Must be at least 1 byte long
     */
    public OpenTSDB(InetSocketAddress address, Charset charset, int bufferSize){
        this.charset = charset;
        this.address = address;
        this.bufferSize = bufferSize;
    }

    /**
     * Creates a connection.
     * @throws SocketException       if the socket could not be opened, or the socket could not bind to the specified local port.
     * @throws IllegalStateException if the client is already connected
     * @throws IOException           if there is an error connecting
     */
    public void connect() throws SocketException, IOException{
        if (writer != null) {
            throw new IllegalStateException("Already connected");
        }
        //TODO: could probably be simplified to just send UDPDatagrams rather that use an OutputStream
        outputstream = new UDPOutputStream(address.getAddress(), address.getPort(), bufferSize);
        writer = new BufferedWriter(new OutputStreamWriter(outputstream, charset));
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
            writer.flush();
            outputstream.close();
            writer.close();
        }
        writer = null;
    }

    protected String sanitize(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }
}
