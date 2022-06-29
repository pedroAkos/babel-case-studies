package statemachine;

import app.Application;
import consensus.MultiPaxos;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import statemachine.ipc.ExecuteBatchNotification;
import statemachine.ipc.LeaderChangeNotification;
import statemachine.ipc.SubmitBatchRequest;
import statemachine.network.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import statemachine.ops.OpBatch;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StateMachine extends GenericProtocol {

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "MultiPaxosSM";

    public static final String ADDRESS_KEY = "frontend_address";
    public static final String PEER_PORT_KEY = "frontend_peer_port";

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    protected final int PEER_PORT;

    protected final InetAddress self;
    protected final Application app;
    private final int opPrefix;
    protected int peerChannel;
    private int opCounter;

    //Forwarded
    private final Queue<Pair<Long, OpBatch>> pendingBatches;
    private Host leader;

    public StateMachine(Properties props, Application app) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.app = app;
        this.PEER_PORT = Integer.parseInt(props.getProperty(PEER_PORT_KEY));

        self = InetAddress.getByName(props.getProperty(ADDRESS_KEY));
        opPrefix = ByteBuffer.wrap(self.getAddress()).getInt();
        opCounter = 0;
        leader = null;
        pendingBatches = new ConcurrentLinkedQueue<>();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        //Peer
        Properties peerProps = new Properties();
        peerProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(PEER_PORT));

        peerChannel = createChannel(TCPChannel.NAME, peerProps);
        registerMessageSerializer(peerChannel, PeerBatchMessage.MSG_CODE, PeerBatchMessage.serializer);
        registerMessageHandler(peerChannel, PeerBatchMessage.MSG_CODE, this::onPeerBatchMessage, this::uponMessageFailed);
        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);

        //Consensus
        subscribeNotification(LeaderChangeNotification.NOTIFICATION_ID, this::onMembershipChange);
        subscribeNotification(ExecuteBatchNotification.NOTIFICATION_ID, this::onExecuteBatch);
    }

    protected long nextId() {
        //Message id is constructed using the server ip and a local counter (makes it unique and sequential)
        opCounter++;
        return ((long) opCounter << 32) | (opPrefix & 0xFFFFFFFFL);
    }

    /* ----------------------------------------------- ------------- ------------------------------------------ */
    /* ----------------------------------------------- APP INTERFACE ------------------------------------------ */
    /* ----------------------------------------------- ------------- ------------------------------------------ */
    public void submitOperation(byte[] op, OpType type) {
        synchronized (this) {
            long internalId = nextId();
            OpBatch batch = new OpBatch(internalId, self, Collections.singletonList(op));
            pendingBatches.add(Pair.of(internalId, batch));
            sendPeerWriteMessage(new PeerBatchMessage(batch), leader);
        }
    }

    private void connectAndSendPendingBatchesToWritesTo() {
        if (!leader.getAddress().equals(self))
            openConnection(leader, peerChannel);
        pendingBatches.forEach(b -> sendPeerWriteMessage(new PeerBatchMessage(b.getRight()), leader));
    }

    private void sendPeerWriteMessage(PeerBatchMessage msg, Host destination) {
        if (destination.getAddress().equals(self)) onPeerBatchMessage(msg, destination, getProtoId(), peerChannel);
        else sendMessage(peerChannel, msg, destination);
    }

    /* ----------------------------------------------- ----------- ----------------------------------------------- */
    /* ----------------------------------------------- PEER EVENTS ----------------------------------------------- */
    /* ----------------------------------------------- ----------- ----------------------------------------------- */

    protected void onPeerBatchMessage(PeerBatchMessage msg, Host from, short sProto, int channel) {
        sendRequest(new SubmitBatchRequest(msg.getBatch()), MultiPaxos.PROTOCOL_ID);
    }

    protected void onOutConnectionUp(OutConnectionUp event, int channel) {
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            logger.debug("Connected to writesTo " + event);
        } else {
            logger.warn("Unexpected connectionUp, ignoring and closing: " + event);
            closeConnection(peer, peerChannel);
        }
    }

    protected void onOutConnectionDown(OutConnectionDown event, int channel) {
        //logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            logger.warn("Lost connection to writesTo, re-connecting: " + event);
            connectAndSendPendingBatchesToWritesTo();
        }
    }

    protected void onOutConnectionFailed(OutConnectionFailed<Void> event, int channel) {
        logger.info(event);
        Host peer = event.getNode();
        if (peer.equals(leader)) {
            logger.warn("Connection failed to writesTo, re-trying: " + event);
            connectAndSendPendingBatchesToWritesTo();
        } else {
            logger.warn("Unexpected connectionFailed, ignoring: " + event);
        }
    }

    private void onInConnectionDown(InConnectionDown event, int channel) {
        logger.debug(event);
    }

    private void onInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    /* ------------------------------------------- ------------- ------------------------------------------- */
    /* ------------------------------------------- CONSENSUS OPS ------------------------------------------- */
    /* ------------------------------------------- ------------- ------------------------------------------- */

    protected void onExecuteBatch(ExecuteBatchNotification not, short from) {
        if (not.getBatch().getIssuer().equals(self)) {
            Pair<Long, OpBatch> ops = pendingBatches.poll();
            if (ops == null || ops.getLeft() != not.getBatch().getBatchId()) {
                logger.error("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
                logger.error(pendingBatches);
                throw new AssertionError("Expected " + not.getBatch().getBatchId() + ". Got " + ops);
            }
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, true));
        } else {
            not.getBatch().getOps().forEach(op -> app.executeOperation(op, false));
        }
    }

    protected void onMembershipChange(LeaderChangeNotification notification, short emitterId) {
        //Writes to changed
        if (leader == null || !notification.getLeader().equals(leader.getAddress())) {
            //Close old writesTo
            if (leader != null && !leader.getAddress().equals(self))
                closeConnection(leader, peerChannel);
            //Update and open to new writesTo
            leader = new Host(notification.getLeader(), PEER_PORT);
            logger.info("New writesTo: " + leader.getAddress());
            connectAndSendPendingBatchesToWritesTo();
        }
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }

    public enum OpType {STRONG_READ, WRITE}

}
