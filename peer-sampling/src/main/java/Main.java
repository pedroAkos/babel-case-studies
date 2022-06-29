import dissemination.flood.FloodGossip;
import dissemination.requests.BroadcastRequest;
import dissemination.requests.DeliverReply;
import hyparview.HyparView;
import membership.PeerSampling;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.*;
import java.security.InvalidParameterException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;


public class Main {

    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    private static final Logger logger = LogManager.getLogger(Main.class);

    private static final String DEFAULT_CONF = "config.properties";

    public static void main(String[] args) throws Exception {
        Babel babel = Babel.getInstance();
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);
        addInterfaceIp(props);
        String address = props.getProperty("address");
        String port = props.getProperty("port");
        Host self = new Host(InetAddress.getByName(address), Short.parseShort(port));
        //PeerSampling sampling = new PeerSampling(props);
        logger.info("Hello, I am {}", self);
        HyparView sampling = new HyparView(TCPChannel.NAME, props, self);
        babel.registerProtocol(sampling);
        sampling.init(props);
        FloodGossip flood = new FloodGossip(props, sampling.getChannel(), sampling.getProtoId());
        babel.registerProtocol(flood);
        flood.init(props);
        DisseminationConsumer dissemination = new DisseminationConsumer(flood.getProtoId(), props);
        babel.registerProtocol(dissemination);
        dissemination.init(props);
        babel.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));
    }

    public static String getIpOfInterface(String interfaceName) throws SocketException {
        NetworkInterface networkInterface = NetworkInterface.getByName(interfaceName);
        System.out.println(networkInterface);
        Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
        InetAddress currentAddress;
        while (inetAddress.hasMoreElements()) {
            currentAddress = inetAddress.nextElement();
            if (currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress()) {
                return currentAddress.getHostAddress();
            }
        }
        return null;
    }

    public static void addInterfaceIp(Properties props) throws SocketException, InvalidParameterException {
        String interfaceName;
        if ((interfaceName = props.getProperty("interface")) != null) {
            String ip = getIpOfInterface(interfaceName);
            if (ip != null)
                props.setProperty("address", ip);
            else {
                throw new InvalidParameterException("Property interface is set to " + interfaceName + ", but has no ip");
            }
        }
    }


    public static class DisseminationConsumer extends GenericProtocol {

        public static class Timer extends ProtoTimer {

            public Timer(short id) {
                super(id);
            }

            @Override
            public ProtoTimer clone() {
                return this;
            }
        }

        private static final Logger logger = LogManager.getLogger(DisseminationConsumer.class);


        public static final String PROTO_NAME = "DisseminationConsumer";
        public static final short PROTO_ID = 11;

        private static final short timerid = 1;

        private int seqNum = 0;
        private final Host self;
        private final short disseminationProto;

        private final int payloadSize;
        private final Random rnd;
        private final int maxMsgs;

        public DisseminationConsumer(short disseminationProto, Properties properties) throws HandlerRegistrationException, UnknownHostException {
            super(PROTO_NAME, PROTO_ID);
            String address = properties.getProperty("address");
            String port = properties.getProperty("port");
            this.self = new Host(InetAddress.getByName(address), Short.parseShort(port));;
            this.disseminationProto = disseminationProto;

            this.payloadSize = Integer.parseInt(properties.getProperty("payloadSize", "0"));
            this.rnd = new Random();

            this.maxMsgs = Integer.parseInt(properties.getProperty("maxMsgs", "0"));

            registerReplyHandler(DeliverReply.REPLY_ID, this::uponDeliver);

            registerTimerHandler(timerid, this::uponSendMsg);
        }

        private void uponDeliver(DeliverReply reply, short sourceProto) {
            byte[] msg;
            if(payloadSize > 0) {
                msg = new byte[reply.getMsg().length - payloadSize];
                System.arraycopy(reply.getMsg(), payloadSize-1, msg, 0, reply.getMsg().length - payloadSize);
            } else
                msg = reply.getMsg();

            logger.info("Received: {}", new String(msg));
        }

        private void uponSendMsg(Timer timer, long timerId) {
            if(maxMsgs == 0 || seqNum < maxMsgs) {
                String tosend = String.format("Hello from %s seq num: %d", self, seqNum++);
                byte[] toSend;
                if (payloadSize > 0) {
                    byte[] payload = new byte[payloadSize];
                    rnd.nextBytes(payload);
                    toSend = new byte[(payloadSize + tosend.getBytes().length)];
                    System.arraycopy(payload, 0, toSend, 0, payloadSize);
                    System.arraycopy(tosend.getBytes(), 0, toSend, payloadSize - 1, tosend.getBytes().length);
                } else
                    toSend = tosend.getBytes();

                sendRequest(new BroadcastRequest(toSend), disseminationProto);
                logger.info("Sent: {}", tosend);
            }
        }

        @Override
        public void init(Properties props) throws HandlerRegistrationException, IOException {
            int disseminationPeriod = Integer.parseInt(props.getProperty("disseminationPeriod", "2000"));
            int disseminationStart = Integer.parseInt(props.getProperty("disseminationStart", "2000"));
            setupPeriodicTimer(new Timer(timerid), disseminationStart, disseminationPeriod);
        }
    }

}
