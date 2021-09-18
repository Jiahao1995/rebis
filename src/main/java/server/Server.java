package server;

import server.handlers.KvOptHandler;
import server.reactor.AbstractNioChannel;
import server.reactor.ChannelHandler;
import server.reactor.Dispatcher;
import server.reactor.NioDatagramChannel;
import server.reactor.NioReactor;
import server.reactor.ThreadPoolDispatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Server
{
    private NioReactor reactor;
    private final List<AbstractNioChannel> channels;
    private final Dispatcher dispatcher;

    public Server()
    {
        channels = new ArrayList<>();
        dispatcher = new ThreadPoolDispatcher(2);
    }

    public void start()
            throws IOException
    {
        reactor = new NioReactor(dispatcher);
        KvOptHandler kvOptHandler = new KvOptHandler();
        reactor.registerChannel(udpChannel(44221, kvOptHandler))
                .start();
    }

    public void stop()
            throws IOException, InterruptedException
    {
        reactor.stop();
        dispatcher.stop();
        for (AbstractNioChannel channel : channels) {
            channel.getJavaChannel().close();
        }
    }

    private AbstractNioChannel udpChannel(int port, ChannelHandler handler)
            throws IOException
    {
        NioDatagramChannel channel = new NioDatagramChannel(port, handler);
        channel.bind();
        channels.add(channel);
        return channel;
    }

    public static void main(String[] args)
            throws IOException
    {
        new Server().start();
    }
}
