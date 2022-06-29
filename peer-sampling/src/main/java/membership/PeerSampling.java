package membership;

import membership.messages.PullMessage;
import membership.messages.PushMessage;
import membership.requests.GetPeerReply;
import membership.requests.GetPeerRequest;
import membership.timers.InfoTimer;
import membership.timers.PushTimer;
import membership.timers.ReconnectTimer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class PeerSampling extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PeerSampling.class);

    private final HashMap<Short,GetPeerRequest> requesters = new HashMap<>();

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "PeerSampling";


    enum ViewPropagation {PUSH, PUSH_PULL}

    enum PeerSelection {RAND, TAIL}

    private final int channelId;
    private final Host self;
    private View view;

    private final int T;
    private final int c;
    private final int H;
    private final int S;
    private final ViewPropagation viewPropagation;
    private final PeerSelection peerSelection;
    private final int RECONNECT_TIME;

    public PeerSampling(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        String address = props.getProperty("address");
        String port = props.getProperty("port");
        this.self = new Host(InetAddress.getByName(address), Short.parseShort(port));

        this.RECONNECT_TIME = Integer.parseInt(props.getProperty("reconnect_time"));
        this.T = Integer.parseInt(props.getProperty("T"));
        this.c = Integer.parseInt(props.getProperty("c"));
        this.H = Integer.parseInt(props.getProperty("H"));
        this.S = Integer.parseInt(props.getProperty("S"));
        this.viewPropagation = ViewPropagation.valueOf(props.getProperty("view_propagation"));
        this.peerSelection = PeerSelection.valueOf(props.getProperty("peer_selection"));


        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        this.channelId = createChannel(TCPChannel.NAME, channelProps);

        registerMessageSerializer(channelId, PushMessage.MSG_ID, PushMessage.serializer);
        registerMessageSerializer(channelId, PullMessage.MSG_ID, PullMessage.serializer);
        registerMessageHandler(channelId, PushMessage.MSG_ID, this::uponPushMessage, this::uponMsgFail);
        registerMessageHandler(channelId, PullMessage.MSG_ID, this::uponPullMessage, this::uponMsgFail);

        registerTimerHandler(PushTimer.TIMER_ID, this::uponPushTimer);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);
        registerTimerHandler(ReconnectTimer.TIMER_ID, this::uponReconnectTimer);

        registerRequestHandler(GetPeerRequest.REQ_ID, this::uponGetPeer);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
    }

    public int getChannel() {
        return channelId;
    }

    @Override
    public void init(Properties props) {
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElements = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElements[0]), Short.parseShort(hostElements[1]));
                this.view = new View(peerSelection, self, contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        } else {
            this.view = new View(peerSelection, self);
        }
        setupPeriodicTimer(new PushTimer(), T, T);

        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponPushTimer(PushTimer timer, long timerId) {
        Host p = view.selectPeer();
        if (p != null) {
            List<Pair<Host, Integer>> buffer = createBuffer();
            sendMessage(new PushMessage(buffer), p);
        }
        if (viewPropagation == ViewPropagation.PUSH)
            view.increaseAge();
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponPushMessage(PushMessage msg, Host from, short sourceProto, int channelId) {
        if (viewPropagation == ViewPropagation.PUSH_PULL) {
            List<Pair<Host, Integer>> buffer = createBuffer();
            sendMessage(new PullMessage(buffer), from, TCPChannel.CONNECTION_IN);
        }
        Pair<List<Host>, List<Host>> changes = view.select(c, H, S, msg.getBuffer());
        view.increaseAge();

        changes.getKey().forEach(this::openConnection);
        changes.getValue().forEach(this::closeConnection);
        requesters.forEach((proto, req) -> {
            sendReply(new GetPeerReply(view.getPeer(req.howMany())), proto);
        });
    }

    private List<Pair<Host, Integer>> createBuffer() {
        List<Pair<Host, Integer>> buffer = new LinkedList<>();
        buffer.add(Pair.of(self, 0));
        view.permute();
        view.moveOldestToEnd(H);
        buffer.addAll(view.head(c / 2 + 1));
        return buffer;
    }

    private void uponPullMessage(PullMessage msg, Host from, short sourceProto, int channelId) {
        Pair<List<Host>, List<Host>> changes = view.select(c, H, S, msg.getBuffer());
        view.increaseAge();

        changes.getKey().forEach(this::openConnection);
        changes.getValue().forEach(this::closeConnection);
        requesters.forEach((proto, req) -> {
            sendReply(new GetPeerReply(view.getPeer(req.howMany())), proto);
        });
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- Requests ------------------------------------- */
    private void uponGetPeer(GetPeerRequest req, short sourceProto) {
        requesters.put(sourceProto, req);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.trace("Connection to {} is up", event.getNode());
        View.NodeDescriptor descriptor = view.getNodeDescriptor(event.getNode());
        if (descriptor != null)
            descriptor.connected = true;
        else
            closeConnection(event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down cause {}", event.getNode(), event.getCause());
        View.NodeDescriptor descriptor = view.getNodeDescriptor(event.getNode());
        if (descriptor != null) {
            descriptor.connected = false;
            openConnection(event.getNode());
        }
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        View.NodeDescriptor descriptor = view.getNodeDescriptor(event.getNode());
        if (descriptor != null)
            setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_TIME);
    }

    private void uponReconnectTimer(ReconnectTimer timer, long l) {
        logger.debug("Reconnecting to {}", timer.getHost());
        View.NodeDescriptor descriptor = view.getNodeDescriptor(timer.getHost());
        if (descriptor != null)
            openConnection(timer.getHost());
    }

    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder();
        sb.append("View: ").append(view);
        logger.info(sb);
    }
}
