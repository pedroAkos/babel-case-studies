package dissemination.flood;

import dissemination.messages.GossipMessage;
import dissemination.requests.BroadcastRequest;
import dissemination.requests.DeliverReply;
import hyparview.notifications.NeighDown;
import hyparview.notifications.NeighUp;
import membership.requests.GetPeerReply;
import membership.requests.GetPeerRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import dissemination.utils.HashProducer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class FloodGossip extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(FloodGossip.class);

    public static final String PROTO_NAME = "FloodGossip";
    public static final short PROTO_ID = 310;


    private final Host myself;
    private Set<Host> neighbours = new HashSet<>();
    private final Set<Integer> received;


    private final HashProducer hashProducer;

    public FloodGossip(Properties properties, int channelId, short peersampling) throws IOException, HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        String address = properties.getProperty("address");
        String port = properties.getProperty("port");
        this.myself =  new Host(InetAddress.getByName(address), Short.parseShort(port));
        this.hashProducer = new HashProducer(myself);
        received = new HashSet<>();


        registerSharedChannel(channelId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);

        /*--------------------- Register Reply Handlers -------------------------------- */
        registerReplyHandler(GetPeerReply.REP_ID, this::uponGetPeerReply);

        /*--------------------- Register Notification Handlers ------------------------ */
        subscribeNotification(NeighUp.NOTIFICATION_ID, this::uponNeighUp);
        subscribeNotification(NeighDown.NOTIFICATION_ID, this::uponNeighDown);

        sendRequest(new GetPeerRequest(Integer.parseInt(properties.getProperty("fanout"))), peersampling);

    }


    /*--------------------------------- Messages ---------------------------------------- */
    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received {} from {}", msg, from);
        if(received.add(msg.getMid())) {
            sendReply(new DeliverReply(msg.getContent()), msg.getToDeliver());
            neighbours.forEach(host ->
            {
                if(!host.equals(from)) {
                    sendMessage(msg, host);
                    logger.trace("Sent {} to {}", msg, host);
                }
            });
        }
    }


    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        int mid = hashProducer.hash(request.getMsg());
        GossipMessage msg = new GossipMessage(mid, 0, sourceProto, request.getMsg());
        uponReceiveGossip(msg, myself, PROTO_ID, -1);

    }

    /*--------------------------------- Replies ---------------------------------------- */
    private void uponGetPeerReply(GetPeerReply reply, short sourceProto) {
        neighbours = new HashSet(reply.peers());
    }


    /*--------------------------------- Notificaitons ---------------------------------------- */
    private void uponNeighUp(NeighUp not, short sourceProto) {
        neighbours.add(not.getPeer());
        logger.info("New neighbour {}, curr view: {}", not.getPeer(), neighbours);
    }

    private void uponNeighDown(NeighDown not, short sourceProto) {
        neighbours.remove(not.getPeer());
        logger.info("Bad neighbour {}, curr view: {}", not.getPeer(), neighbours);
    }


    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }
}
