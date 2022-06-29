package consensus;

import consensus.messages.*;
import consensus.timers.LeaderTimer;
import consensus.timers.NoOpTimer;
import consensus.timers.ReconnectTimer;
import consensus.utils.AcceptedValue;
import consensus.utils.InstanceState;
import consensus.utils.SeqN;
import consensus.values.AppOpBatch;
import consensus.values.NoOpValue;
import consensus.values.PaxosValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.MultithreadedTCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import statemachine.ipc.ExecuteBatchNotification;
import statemachine.ipc.LeaderChangeNotification;
import statemachine.ipc.SubmitBatchRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class DistPaxos extends GenericProtocol {

    public static final short PROTOCOL_ID = 400;
    public static final String PROTOCOL_NAME = "SDistNoPL";
    public static final String ADDRESS_KEY = "consensus_address";
    public static final String PORT_KEY = "consensus_port";
    public static final String LEADER_TIMEOUT_KEY = "leader_timeout";
    public static final String INITIAL_MEMBERSHIP_KEY = "initial_membership";
    public static final String RECONNECT_TIME_KEY = "reconnect_time";
    private static final Logger logger = LogManager.getLogger(DistPaxos.class);
    private static final int INITIAL_MAP_SIZE = 10000;

    private final int LEADER_TIMEOUT;
    private final int NOOP_SEND_INTERVAL;
    private final int RECONNECT_TIME;

    private final Host self;
    private final List<Host> peers;
    private final int quorumSize;
    private final Queue<AppOpBatch> waitingAppOps = new LinkedList<>();
    private final Map<Integer, InstanceState> instances = new HashMap<>(INITIAL_MAP_SIZE);
    private int currentInstance; // Highest non-decided

    //Leadership
    private SeqN currentSN;
    private boolean amCurrentLeader;
    //Timers
    private long noOpTimer;
    private long lastLeaderOp;

    public DistPaxos(Properties props) throws UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        currentSN = new SeqN(-1, null);
        amCurrentLeader = false;

        currentInstance = 0;

        self = new Host(InetAddress.getByName(props.getProperty(ADDRESS_KEY)),
                Integer.parseInt(props.getProperty(PORT_KEY)));

        this.RECONNECT_TIME = Integer.parseInt(props.getProperty(RECONNECT_TIME_KEY));
        this.LEADER_TIMEOUT = Integer.parseInt(props.getProperty(LEADER_TIMEOUT_KEY));
        this.NOOP_SEND_INTERVAL = LEADER_TIMEOUT / 3;

        peers = new LinkedList<>();
        String[] membership = props.getProperty(INITIAL_MEMBERSHIP_KEY).split(",");
        quorumSize = membership.length / 2 + 1;
        for (String s : membership)
            peers.add(new Host(InetAddress.getByName(s), self.getPort()));

        if (!peers.contains(self)) {
            logger.error("Non seed starting in active state");
            throw new AssertionError("Non seed starting in active state");
        }
        peers.remove(self);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

        Properties peerProps = new Properties();
        peerProps.put(MultithreadedTCPChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
        peerProps.setProperty(TCPChannel.PORT_KEY, props.getProperty(PORT_KEY));
        int peerChannel = createChannel(TCPChannel.NAME, peerProps);

        registerMessageSerializer(peerChannel, AcceptedMsg.MSG_CODE, AcceptedMsg.serializer);
        registerMessageSerializer(peerChannel, AcceptMsg.MSG_CODE, AcceptMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareMsg.MSG_CODE, PrepareMsg.serializer);
        registerMessageSerializer(peerChannel, PrepareOkMsg.MSG_CODE, PrepareOkMsg.serializer);
        registerMessageSerializer(peerChannel, DecisionMsg.MSG_CODE, DecisionMsg.serializer);

        registerMessageHandler(peerChannel, AcceptedMsg.MSG_CODE, this::uponAcceptedMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, AcceptMsg.MSG_CODE, this::uponAcceptMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareMsg.MSG_CODE, this::uponPrepareMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, PrepareOkMsg.MSG_CODE, this::uponPrepareOkMsg, this::uponMessageFailed);
        registerMessageHandler(peerChannel, DecisionMsg.MSG_CODE, this::uponDecisionMsg, this::uponMessageFailed);

        registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

        registerTimerHandler(LeaderTimer.TIMER_ID, this::onLeaderTimer);
        registerTimerHandler(NoOpTimer.TIMER_ID, this::onNoOpTimer);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);

        registerRequestHandler(SubmitBatchRequest.REQUEST_ID, this::onSubmitBatch);

        peers.forEach(this::openConnection);
        setupPeriodicTimer(LeaderTimer.instance, LEADER_TIMEOUT, LEADER_TIMEOUT / 3);
        lastLeaderOp = System.currentTimeMillis();

        logger.info("DistPaxos starting: " + peers + " " + quorumSize);
    }

    private void onLeaderTimer(LeaderTimer timer, long timerId) {
        if (!amCurrentLeader && (System.currentTimeMillis() - lastLeaderOp > LEADER_TIMEOUT))
            tryTakeLeadership();
    }

    private void tryTakeLeadership() { //Take leadership, send prepare
        logger.info("Attempting to take leadership...");
        InstanceState instance = instances.computeIfAbsent(currentInstance, InstanceState::new);
        SeqN newSeqN = new SeqN(currentSN.getCounter() + 1, self);
        instance.prepareResponses.put(newSeqN, new HashSet<>());
        PrepareMsg pMsg = new PrepareMsg(instance.iN, newSeqN);
        peers.forEach(h -> sendMessage(pMsg, h));
        uponPrepareMsg(pMsg, self, getProtoId(), -1);
    }

    private void uponPrepareMsg(PrepareMsg msg, Host from, short sourceProto, int channel) {
        if (msg.sN.greaterThan(currentSN)) {
            newHighestSn(msg.iN, msg.sN);

            List<AcceptedValue> values = new LinkedList<>();
            for (int i = msg.iN; i <= currentInstance; i++) {
                InstanceState inst = instances.get(i);
                if (inst != null && inst.acceptedValue != null)
                    values.add(new AcceptedValue(i, inst.highestAccept, inst.acceptedValue));
            }
            PrepareOkMsg response = new PrepareOkMsg(msg.iN, msg.sN, values);
            if(from.equals(self)) uponPrepareOkMsg(response, self, getProtoId(), -1);
            else sendMessage(response, from);
            lastLeaderOp = System.currentTimeMillis();
        }
    }

    private void newHighestSn(int iN, SeqN sN) {
        logger.info("New highest instance leader: iN:" + iN + ", " + sN);
        currentSN = sN;
        if (amCurrentLeader) {
            amCurrentLeader = false;
            cancelTimer(noOpTimer);
            waitingAppOps.clear();
        }
        triggerNotification(new LeaderChangeNotification(currentSN.getNode().getAddress()));
    }

    private void uponPrepareOkMsg(PrepareOkMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.get(msg.iN);
        Set<Host> oks = instance.prepareResponses.get(msg.sN);
        if (currentSN.greaterThan(msg.sN) || oks == null)
            return;

        oks.add(from);
        for (AcceptedValue acceptedValue : msg.acceptedValues) {
            InstanceState inst = instances.computeIfAbsent(acceptedValue.instance, InstanceState::new);
            if (inst.highestAccept == null || acceptedValue.sN.greaterThan(inst.highestAccept))
                inst.accept(acceptedValue.sN, acceptedValue.value);
        }

        if (oks.size() == quorumSize) {
            logger.info("I am leader now! @ instance " + msg.iN);
            instance.prepareResponses.remove(msg.sN);
            amCurrentLeader = true;
            sendAccept(instance.iN);
        }
    }

    private void onNoOpTimer(NoOpTimer timer, long timerId) {
        if (amCurrentLeader && instances.get(currentInstance) == null)
            sendAccept(currentInstance);
    }

    private void sendAccept(int instance) {
        InstanceState ins = instances.computeIfAbsent(instance, InstanceState::new);

        PaxosValue val;
        if (ins.acceptedValue != null) val = ins.acceptedValue;
        else if (!waitingAppOps.isEmpty()) val = waitingAppOps.remove();
        else val = new NoOpValue();

        AcceptMsg acceptMsg = new AcceptMsg(ins.iN, currentSN, val);
        peers.forEach(h -> sendMessage(acceptMsg, h));
        uponAcceptMsg(acceptMsg, self, this.getProtoId(), -1);
    }

    private void uponAcceptMsg(AcceptMsg msg, Host from, short sourceProto, int channel) {
        if (msg.sN.lesserThan(currentSN))
            return;

        if (msg.sN.greaterThan(currentSN))
            newHighestSn(msg.iN, msg.sN);

        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);
        instance.accept(msg.sN, msg.value);

        AcceptedMsg acceptedMsg = new AcceptedMsg(msg.iN, msg.sN, msg.value);
        if(from.equals(self)) uponAcceptedMsg(acceptedMsg, self, this.getProtoId(), -1);
        else sendMessage(acceptedMsg, from);

        lastLeaderOp = System.currentTimeMillis();
    }

    private void uponAcceptedMsg(AcceptedMsg msg, Host from, short sourceProto, int channel) {
        if (msg.sN.lesserThan(currentSN))
            return;

        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);
        int accepteds = instance.registerAccepted(msg.sN, msg.value, from);

        if (!instance.isDecided() && (accepteds >= quorumSize)) { //We have quorum!
            DecisionMsg dMsg = new DecisionMsg(instance.iN, instance.highestAccept, instance.acceptedValue);
            uponDecisionMsg(dMsg, self, getProtoId(), -1);
            peers.forEach(p -> sendMessage(dMsg, p));

            if (amCurrentLeader) {
                if (!waitingAppOps.isEmpty() || instances.get(currentInstance) != null)
                    sendAccept(currentInstance);
                else
                    noOpTimer = setupTimer(NoOpTimer.instance, NOOP_SEND_INTERVAL);
            }
        }
    }

    private void uponDecisionMsg(DecisionMsg msg, Host from, short sourceProto, int channel) {
        InstanceState instance = instances.computeIfAbsent(msg.iN, InstanceState::new);

        if (!instance.isDecided()) { //We have quorum!
            instance.accept(msg.sN, msg.value);
            instance.markDecided();
            currentInstance++;

            if (instance.acceptedValue.type == PaxosValue.Type.APP_BATCH) //Not a NO_OP
                triggerNotification(new ExecuteBatchNotification(((AppOpBatch) instance.acceptedValue).getBatch()));
        }
        lastLeaderOp = System.currentTimeMillis();
    }

    public void onSubmitBatch(SubmitBatchRequest not, short from) {
        if (amCurrentLeader) {
            waitingAppOps.add(new AppOpBatch(not.getBatch()));
            if (instances.get(currentInstance) == null) {
                cancelTimer(noOpTimer);
                sendAccept(currentInstance);
            }
        } else if (currentSN.getNode().equals(self))
            waitingAppOps.add(new AppOpBatch(not.getBatch()));
        else
            logger.warn("Received " + not + " without being leader, ignoring.");
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channel) {
        logger.warn(event);
        setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
        logger.warn("Connection failed to " + ev.getNode() + ", cause: " + ev.getCause().getMessage());
        setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_TIME);
    }

    private void onReconnectTimer(ReconnectTimer timer, long timerId) {
        openConnection(timer.getHost());
    }

    private void uponInConnectionUp(InConnectionUp event, int channel) {
        logger.debug(event);
    }

    private void uponInConnectionDown(InConnectionDown event, int channel) {
        logger.info(event);
    }

    private void uponMessageFailed(ProtoMessage msg, Host host, short i, Throwable throwable, int i1) {
        logger.warn("Failed: " + msg + ", to: " + host + ", reason: " + throwable.getMessage());
    }
}