package statemachine.ipc;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.net.InetAddress;
import java.util.List;

public class LeaderChangeNotification extends ProtoNotification {

    public final static short NOTIFICATION_ID = 102;

    private final InetAddress leader;

    public LeaderChangeNotification(InetAddress leader) {
        super(NOTIFICATION_ID);
        this.leader = leader;
    }

    public InetAddress getLeader() {
        return leader;
    }
}
