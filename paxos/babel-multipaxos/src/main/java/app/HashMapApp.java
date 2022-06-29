package app;

import app.networking.RequestDecoder;
import app.networking.RequestMessage;
import app.networking.ResponseEncoder;
import app.networking.ResponseMessage;
import consensus.DistPaxos;
import consensus.MultiPaxos;
import statemachine.StateMachine;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HashMapApp implements Application {

    private static final Logger logger = LogManager.getLogger(HashMapApp.class);
    private final ConcurrentMap<Integer, Pair<Integer, Channel>> opMapper;
    private final AtomicInteger idCounter;
    private final StateMachine frontend;

    //State
    private int nWrites;
    private ConcurrentMap<String, byte[]> store;

    public HashMapApp(Properties configProps) throws IOException, ProtocolAlreadyExistsException,
            HandlerRegistrationException, InterruptedException {

        store = new ConcurrentHashMap<>();
        nWrites = 0;

        idCounter = new AtomicInteger(0);
        opMapper = new ConcurrentHashMap<>();
        int port = Integer.parseInt(configProps.getProperty("app_port"));
        Babel babel = Babel.getInstance();
        String alg = configProps.getProperty("algorithm");

        frontend = new StateMachine(configProps, this);
        GenericProtocol consensusProto;
        switch (alg) {
            case "dist":
                consensusProto = new DistPaxos(configProps);
                break;
            case "multi":
                consensusProto = new MultiPaxos(configProps);
                break;
            default:
                logger.error("Unknown algorithm: " + alg);
                System.exit(-1);
                return;
        }

        babel.registerProtocol(frontend);
        babel.registerProtocol(consensusProto);

        frontend.init(configProps);
        consensusProto.init(configProps);

        babel.start();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RequestDecoder(), new ResponseEncoder(), new ServerHandler());
                        }
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            logger.info("Listening: " + f.channel().localAddress());
            f.channel().closeFuture().sync();
            logger.info("Server channel closed");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InvalidParameterException, IOException,
            HandlerRegistrationException, ProtocolAlreadyExistsException, InterruptedException {
        Properties props =
                Babel.loadConfig(Arrays.copyOfRange(args, 0, args.length), "config.properties");
        logger.debug(props);
        if (props.containsKey("interface")) {
            String address = getAddress(props.getProperty("interface"));
            if (address == null) return;
            props.setProperty(StateMachine.ADDRESS_KEY, address);
            props.setProperty(MultiPaxos.ADDRESS_KEY, address);
        }
        new HashMapApp(props);
    }

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
            logger.error("No interface named " + inter);
            return null;
        }
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address)
                return currentAddress.getHostAddress();
        }
        logger.error("No ipv4 found for interface " + inter);
        return null;
    }

    //Called by **single** frontend thread
    @Override
    public void executeOperation(byte[] opData, boolean local) {
        HashMapOp op;
        try {
            op = HashMapOp.fromByteArray(opData);
        } catch (IOException e) {
            logger.error("Error decoding opData", e);
            throw new AssertionError("Error decoding opData");
        }
        //logger.info("Exec op: " + op + (local ? "local" : ""));

        Pair<Integer, Channel> opInfo = local ? opMapper.remove(op.getId()) : null;
        if (op.getRequestType() == RequestMessage.WRITE) {
            store.put(op.getRequestKey(), op.getRequestValue());
            nWrites++;
            if (local)
                opInfo.getRight().writeAndFlush(new ResponseMessage(opInfo.getLeft(), new byte[0]));
        } else { //READ
            if (local) {
                opInfo.getRight().writeAndFlush(
                        new ResponseMessage(opInfo.getLeft(), store.getOrDefault(op.getRequestKey(), new byte[0])));
            } //If remote read, nothing to do
        }
    }

    class ServerHandler extends ChannelInboundHandlerAdapter {
        //Called by netty threads
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //logger.info("Client connected: " + ctx.channel().remoteAddress());
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            //logger.info("Client connection lost: " + ctx.channel().remoteAddress());
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught.", cause);
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            logger.info("Unexpected event: " + evt);
            ctx.fireUserEventTriggered(evt);
        }

        //Called by netty threads
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RequestMessage rMsg = (RequestMessage) msg;
            //logger.info("Client op: " + msg);
            if (rMsg.getRequestType() == RequestMessage.WEAK_READ) { //Exec immediately and respond
                byte[] bytes = store.get(rMsg.getRequestKey());
                ctx.channel().writeAndFlush(new ResponseMessage(rMsg.getcId(), bytes == null ? new byte[0] : bytes));
            } else { //Submit to consensus
                int id = idCounter.incrementAndGet();
                opMapper.put(id, Pair.of(rMsg.getcId(), ctx.channel()));
                byte[] opData = HashMapOp.toByteArray(id, rMsg.getRequestType(), rMsg.getRequestKey(), rMsg.getRequestValue());
                frontend.submitOperation(opData, rMsg.getRequestType() == RequestMessage.WRITE ?
                        StateMachine.OpType.WRITE : StateMachine.OpType.STRONG_READ);
            }
        }
    }

}
