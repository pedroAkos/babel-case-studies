package novasys.app;

import com.wuba.wpaxos.ProposeResult;
import com.wuba.wpaxos.comm.GroupSMInfo;
import com.wuba.wpaxos.comm.NodeInfo;
import com.wuba.wpaxos.comm.Options;
import com.wuba.wpaxos.comm.enums.IndexType;
import com.wuba.wpaxos.config.PaxosTryCommitRet;
import com.wuba.wpaxos.node.Node;
import com.wuba.wpaxos.sample.echo.EchoSM;
import com.wuba.wpaxos.sample.echo.EchoSMCtx;
import com.wuba.wpaxos.sample.util.NodeUtil;
import com.wuba.wpaxos.store.config.StoreConfig;
import com.wuba.wpaxos.storemachine.SMCtx;
import com.wuba.wpaxos.storemachine.StateMachine;
import com.wuba.wpaxos.utils.JavaOriTypeWrapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import novasys.app.networking.RequestDecoder;
import novasys.app.networking.RequestMessage;
import novasys.app.networking.ResponseEncoder;
import novasys.app.networking.ResponseMessage;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class HashMapApp implements StateMachine {

    public static final int SMID = 1;

    private final NodeInfo myNode;
    private Node paxosNode;

    private int nWrites;
    private ConcurrentMap<String, byte[]> store;

    private ExecutorService executor;

    public HashMapApp(NodeInfo myNode, List<NodeInfo> nodeInfoList, String rootPath, int hashMapPort) throws Exception {
        this.myNode = myNode;
        store = new ConcurrentHashMap<>();

        executor = Executors.newFixedThreadPool(1000);

        Options options = new Options();
        String logStoragePath = this.makeLogStoragePath(rootPath);
        options.setLogStoragePath(logStoragePath);
        options.setGroupCount(1);
        options.setMyNode(this.myNode);
        options.setNodeInfoList(nodeInfoList);
        options.setUseMembership(true);
        options.setWriteSync(false);
        options.setUseBatchPropose(false);
        options.setIndexType(IndexType.PHYSIC_FILE);
        StoreConfig storeConfig = new StoreConfig(rootPath, null);
        storeConfig.setTransientStorePoolEnable(true);
        options.setStoreConfig(storeConfig);

        GroupSMInfo smInfo = new GroupSMInfo();
        smInfo.setUseMaster(true);
        smInfo.setGroupIdx(0);
        smInfo.getSmList().add(this);
        options.getGroupSMInfoList().add(smInfo);

        this.paxosNode = Node.runNode(options);
        paxosNode.setTimeoutMs(3000);

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

            ChannelFuture f = b.bind(hashMapPort).sync();
            System.out.println("Listening in port " + f.channel().localAddress());
            f.channel().closeFuture().sync();
            System.out.println("Server channel closed");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("arguments num is wrong , they are [rootPath myNode nodeList hashMapPort]");
            System.exit(1);
        }

        String rootPath = args[0];
        NodeInfo myNode = NodeUtil.parseIpPort(args[1]);
        List<NodeInfo> nodeInfoList = NodeUtil.parseIpPortList(args[2]);
        int hashMapPort = Integer.parseInt(args[3]);

        new HashMapApp(myNode, nodeInfoList, rootPath, hashMapPort);
    }

    @Override
    public int getSMID() {
        return SMID;
    }

    //Called by **single** consensus thread
    @Override
    public boolean execute(int groupIdx, long instanceID, byte[] opData, SMCtx smCtx) {
        HashMapOp op;
        try {
            op = HashMapOp.fromByteArray(opData);
        } catch (IOException e) {
            System.err.println("Error decoding opData: " + e);
            throw new AssertionError("Error decoding opData");
        }
        if (op.getRequestType() == RequestMessage.WRITE) {
            store.put(op.getRequestKey(), op.getRequestValue());
            nWrites++;
            if(smCtx != null && smCtx.getpCtx() != null)
                ((HashMapCtX) smCtx.getpCtx()).setResponse(new byte[0]);
        } else { //READ
            if(smCtx != null && smCtx.getpCtx() != null)
                ((HashMapCtX) smCtx.getpCtx()).setResponse(store.getOrDefault(op.getRequestKey(), new byte[0]));
        }
        return true;
    }

    @Override
    public boolean executeForCheckpoint(int groupIdx, long instanceID, byte[] paxosValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getCheckpointInstanceID(int groupIdx) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int lockCheckpointState() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getCheckpointState(int groupIdx, JavaOriTypeWrapper<String> dirPath, List<String> fileList) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void unLockCheckpointState() {
        // TODO Auto-generated method stub
    }

    @Override
    public int loadCheckpointState(int groupIdx, String checkpointTmpFileDirPath, List<String> fileList,
                                   long checkpointInstanceID) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public byte[] beforePropose(int groupIdx, byte[] sValue) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean needCallBeforePropose() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void fixCheckpointByMinChosenInstanceId(long minChosenInstanceID) {
        // TODO Auto-generated method stub
    }

    public String makeLogStoragePath(String rootPath) {
        if (rootPath == null) {
            rootPath = System.getProperty("user.dir");
        }
        String logStoragePath = rootPath + File.separator + myNode.getNodeID() + File.separator + "db";
        File file = new File(logStoragePath);
        file.mkdirs();
        return logStoragePath;
    }

    class ServerHandler extends ChannelInboundHandlerAdapter {

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
            if (rMsg.getRequestType() == RequestMessage.WEAK_READ) { //Exec immediately and respond
                byte[] bytes = store.get(rMsg.getRequestKey());
                ctx.channel().writeAndFlush(new ResponseMessage(rMsg.getcId(), bytes == null ? new byte[0] : bytes));
            } else { //Submit to consensus
                UUID opId = UUID.randomUUID();
                byte[] opData = HashMapOp.toByteArray(opId, rMsg.getRequestType(),
                        rMsg.getRequestKey(), rMsg.getRequestValue());
                executor.submit(() -> {
                    SMCtx sMCtx = new SMCtx();
                    HashMapCtX mapCtX = new HashMapCtX();
                    sMCtx.setSmId(HashMapApp.SMID);
                    sMCtx.setpCtx(mapCtX);

                    ProposeResult proposeResult = paxosNode.propose(0, opData, sMCtx);
                    if (PaxosTryCommitRet.PaxosTryCommitRet_OK.getRet() == proposeResult.getResult()
                            && mapCtX.getResponse() != null) {
                        ctx.writeAndFlush(new ResponseMessage(rMsg.getcId(), mapCtX.getResponse()));
                    }
                });
            }
        }

    }


}
