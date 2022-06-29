import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import network.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PaxosClient extends DB {

    private static final AtomicInteger initCounter = new AtomicInteger();
    private static final ThreadLocal<Channel> threadServer = new ThreadLocal<>();
    private static final Map<Channel, Map<Integer, CompletableFuture<ResponseMessage>>> opCallbacks = new HashMap<>();
    private static AtomicInteger idCounter;
    private static int timeoutMillis;
    private static byte readType;
    private static List<Channel> servers;

    @Override
    public void init() {
        try {
            synchronized (opCallbacks) {
                if (servers == null) {
                    //ONCE
                    timeoutMillis = Integer.parseInt(getProperties().getProperty("timeout_millis"));
                    int serverPort = Integer.parseInt(getProperties().getProperty("app_server_port"));
                    String readProp = getProperties().getProperty("read_type", "strong");
                    if (readProp.equals("weak")) readType = RequestMessage.WEAK_READ;
                    else if (readProp.equals("strong")) readType = RequestMessage.STRONG_READ;
                    idCounter = new AtomicInteger(0);
                    servers = new LinkedList<>();

                    EventLoopGroup workerGroup = new NioEventLoopGroup();
                    Bootstrap b = new Bootstrap();
                    b.group(workerGroup);
                    b.channel(NioSocketChannel.class);
                    b.option(ChannelOption.SO_KEEPALIVE, true);
                    b.handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RequestEncoder(), new ResponseDecoder(), new ClientHandler());
                        }
                    });

                    String[] hosts = getProperties().getProperty("hosts").split(",");
                    List<ChannelFuture> connectFutures = new LinkedList<>();
                    for (String s : hosts) {
                        InetAddress addr = InetAddress.getByName(s);
                        ChannelFuture connect = b.connect(addr, serverPort);
                        connectFutures.add(connect);
                        servers.add(connect.channel());
                        opCallbacks.put(connect.channel(), new ConcurrentHashMap<>());
                    }
                    for (ChannelFuture f : connectFutures)
                        f.sync();
                    System.err.println("Connected to all servers!");
                    //END ONCE
                }
                int threadId = initCounter.incrementAndGet();
                int randIdx = threadId % servers.size();
                threadServer.set(servers.get(randIdx));
            }
        } catch (UnknownHostException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            int id = idCounter.incrementAndGet();
            RequestMessage requestMessage = new RequestMessage(id, readType, key, new byte[0]);
            return executeOperation(requestMessage);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            byte[] value = values.values().iterator().next().toArray();
            int id = idCounter.incrementAndGet();
            RequestMessage requestMessage = new RequestMessage(id, RequestMessage.WRITE, key, value);
            return executeOperation(requestMessage);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return Status.ERROR;
        }
    }

    private Status executeOperation(RequestMessage requestMessage) throws InterruptedException, ExecutionException {
        Channel channel = threadServer.get();
        CompletableFuture<ResponseMessage> future = new CompletableFuture<>();
        opCallbacks.get(channel).put(requestMessage.getcId(), future);
        channel.writeAndFlush(requestMessage);
        try {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            return Status.OK;
        } catch (TimeoutException ex) {
            System.err.println("Op Timed out..." + channel.remoteAddress() + " " + requestMessage.getcId());
            System.exit(1);
            return Status.SERVICE_UNAVAILABLE;
        }
    }

    @Override
    public Status scan(String t, String sK, int rC, Set<String> f, Vector<HashMap<String, ByteIterator>> res) {
        throw new IllegalStateException();
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        throw new IllegalStateException();
    }

    @Override
    public Status delete(String table, String key) {
        throw new IllegalStateException();
    }


    static class ClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            System.err.println("Unexpected event, exiting: " + evt);
            System.exit(1);
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.err.println("Server connection lost, exiting: " + ctx.channel().remoteAddress());
            System.exit(1);
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Exception caught, exiting: " + cause);
            cause.printStackTrace();
            System.exit(1);
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            //System.err.println("Connected to " + ctx.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            //System.err.println("Message received: " + msg);
            ResponseMessage resp = (ResponseMessage) msg;
            opCallbacks.get(ctx.channel()).get(resp.getcId()).complete(resp);
        }
    }
}
