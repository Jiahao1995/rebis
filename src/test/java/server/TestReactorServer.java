package server;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.reactor.SameThreadDispatcher;
import server.reactor.ThreadPoolDispatcher;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * This class tests the Distributed Logging service by starting a Reactor and then sending it
 * concurrent logging requests using multiple clients.
 */
public class TestReactorServer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestReactorServer.class);

    /**
     * Test the application using pooled thread dispatcher.
     *
     * @throws IOException if any I/O error occurs.
     * @throws InterruptedException if interrupted while stopping the application.
     */
    @Test
    public void testAppUsingThreadPoolDispatcher()
            throws IOException, InterruptedException
    {
        LOGGER.info("testAppUsingThreadPoolDispatcher start");
        var loggingServer = new LoggingServer(new ThreadPoolDispatcher(2));
        loggingServer.start();

        assertNotNull(loggingServer);

        var client = new LoggingClient();
        client.start();

        assertNotNull(client);

        // allow clients to send requests. Artificial delay.
        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e) {
            LOGGER.error("sleep interrupted", e);
        }

        client.stop();

        loggingServer.stop();
        LOGGER.info("testAppUsingThreadPoolDispatcher stop");
    }

    /**
     * Test the application using same thread dispatcher.
     *
     * @throws IOException if any I/O error occurs.
     * @throws InterruptedException if interrupted while stopping the application.
     */
    @Test
    public void testAppUsingSameThreadDispatcher()
            throws IOException, InterruptedException
    {
        LOGGER.info("testAppUsingSameThreadDispatcher start");
        var loggingServer = new LoggingServer(new SameThreadDispatcher());
        loggingServer.start();

        assertNotNull(loggingServer);

        var client = new LoggingClient();
        client.start();

        assertNotNull(client);

        // allow clients to send requests. Artificial delay.
        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e) {
            LOGGER.error("sleep interrupted", e);
        }

        client.stop();

        loggingServer.stop();
        LOGGER.info("testAppUsingSameThreadDispatcher stop");
    }
}
