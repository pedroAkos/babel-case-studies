package hyparview.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NeighUp extends ProtoNotification {

    public static final short NOTIFICATION_ID = 401;

    private final Host peer;

    public NeighUp(Host peer) {
        super(NOTIFICATION_ID);
        this.peer = peer;
    }


    public Host getPeer() {
        return peer;
    }
}
