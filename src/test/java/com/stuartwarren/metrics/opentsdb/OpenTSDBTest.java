package com.stuartwarren.metrics.opentsdb;

//FIXME - Copied from graphite

import org.junit.Before;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class OpenTSDBTest {
    private final SocketFactory socketFactory = mock(SocketFactory.class);
    private final InetSocketAddress address = new InetSocketAddress("example.com", 1234);
    private final OpenTSDB opentsdb = new OpenTSDB(address);

    private final Socket socket = mock(Socket.class);
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void setUp() throws Exception {
        when(socket.getOutputStream()).thenReturn(output);

        when(socketFactory.createSocket(any(InetAddress.class),
                                        anyInt())).thenReturn(socket);

    }

    @Test
    public void connectsToOpenTSDB() throws Exception {
        opentsdb.connect();

        verify(socketFactory).createSocket(address.getAddress(), address.getPort());
    }

    @Test
    public void measuresFailures() throws Exception {
        assertThat(opentsdb.getFailures())
                .isZero();
    }

    @Test
    public void disconnectsFromOpenTSDB() throws Exception {
        opentsdb.connect();
        opentsdb.close();

        verify(socket).close();
    }

    @Test
    public void doesNotAllowDoubleConnections() throws Exception {
        opentsdb.connect();
        try {
            opentsdb.connect();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage())
                    .isEqualTo("Already connected");
        }
    }

    @Test
    public void writesValuesToOpenTSDB() throws Exception {
        opentsdb.connect();
        opentsdb.send("name", "value", 100);

        assertThat(output.toString())
                .isEqualTo("name value 100\n");
    }

    @Test
    public void sanitizesNames() throws Exception {
        opentsdb.connect();
        opentsdb.send("name woo", "value", 100);

        assertThat(output.toString())
                .isEqualTo("name-woo value 100\n");
    }

    @Test
    public void sanitizesValues() throws Exception {
        opentsdb.connect();
        opentsdb.send("name", "value woo", 100);

        assertThat(output.toString())
                .isEqualTo("name value-woo 100\n");
    }
}
