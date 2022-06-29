package novasys.app;

import com.github.luohaha.paxos.core.ConfObject;
import com.github.luohaha.paxos.core.Proposer;
import com.github.luohaha.paxos.main.MyPaxos;
import com.github.luohaha.paxos.packet.PacketBean;
import com.github.luohaha.paxos.packet.Value;
import com.github.luohaha.paxos.utils.FileUtils;
import com.google.gson.Gson;
import io.netty.channel.*;
import novasys.app.networking.RequestDecoder;
import novasys.app.networking.RequestMessage;
import novasys.app.networking.ResponseEncoder;
import novasys.app.networking.ResponseMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HashMapApp {

    private final ConcurrentMap<UUID, Pair<Integer, Channel>> opMapper;
    private final AtomicInteger idCounter;
    private int nWrites;
    private ConcurrentMap<String, byte[]> store;

    public HashMapApp(String confFile, String servers) throws InterruptedException, IOException, ClassNotFoundException {

        Gson gson = new Gson();
        ConfObject confObject = gson.fromJson(FileUtils.readFromFile(confFile), ConfObject.class);
        store = new ConcurrentHashMap<>();
        idCounter = new AtomicInteger(0);
        opMapper = new ConcurrentHashMap<>();
        int port = confObject.getAppPort();

        String[] hosts = servers.split(",");
        for(int i = 0;i<hosts.length;i++){
            confObject.getNodes().get(i).setHost(hosts[i]);
        }
        MyPaxos myPaxos = new MyPaxos(confObject);
        Proposer proposer = myPaxos.setGroupId(1, this::executeOperation);

        new Thread(() -> {
            try {
                myPaxos.start();
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.err.println("Failed to start myPaxos");
                e.printStackTrace();
                System.exit(1);
            }
        }).start();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RequestDecoder(), new ResponseEncoder(), new ServerHandler(proposer));
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            //System.out.println("Listening: " + f.channel().localAddress());
            f.channel().closeFuture().sync();
            System.out.println("Server channel closed");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        new HashMapApp(args[0], args[1]);
    }

    //Called by **single** learner thread
    public void executeOperation(byte[] opData) {
        HashMapOp op;
        try {
            op = HashMapOp.fromByteArray(opData);
        } catch (IOException e) {
            System.err.println("Error decoding opData: " + e);
            throw new AssertionError("Error decoding opData");
        }

        Pair<Integer, Channel> opInfo = opMapper.remove(op.getId());
        boolean local = opInfo != null;
        if (op.getRequestType() == RequestMessage.WRITE) {
            store.put(op.getRequestKey(), op.getRequestValue());
            nWrites++;
            if (local) {
                opInfo.getRight().writeAndFlush(new ResponseMessage(opInfo.getLeft(), new byte[0]));
                //System.out.println("Responded to client: " + op);
            }
        } else { //READ
            if (local) {
                opInfo.getRight().writeAndFlush(
                        new ResponseMessage(opInfo.getLeft(), store.getOrDefault(op.getRequestKey(), new byte[0])));
                //System.out.println("Responded to client: " + op);
            } //If remote read, nothing to do
        }
    }

    class ServerHandler extends ChannelInboundHandlerAdapter {
        private final Proposer proposer;
        public ServerHandler(Proposer proposer) {
            this.proposer = proposer;
        }

        //Called by netty threads
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("Client connected: " + ctx.channel().remoteAddress());
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("Client connection lost: " + ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Exception caught: " + cause);
            cause.printStackTrace();
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            System.out.println("Unexpected event: " + evt);
            ctx.fireUserEventTriggered(evt);
        }

        //Called by netty threads
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RequestMessage rMsg = (RequestMessage) msg;
            //System.out.println("--------------------------------------------------------------------------------");
            //System.out.println("Client op: " + msg);
            if (rMsg.getRequestType() == RequestMessage.WEAK_READ) { //Exec immediately and respond
                byte[] bytes = store.get(rMsg.getRequestKey());
                ctx.channel().writeAndFlush(new ResponseMessage(rMsg.getcId(), bytes == null ? new byte[0] : bytes));
            } else { //Submit to consensus
                UUID opId = UUID.randomUUID();
                opMapper.put(opId, Pair.of(rMsg.getcId(), ctx.channel()));
                byte[] opData = HashMapOp.toByteArray(opId, rMsg.getRequestType(), rMsg.getRequestKey(), rMsg.getRequestValue());
                proposer.sendPacket(new PacketBean("SubmitPacket", new Value(UUID.randomUUID(), opData)));
            }
        }

    }

}
