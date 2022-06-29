package membership.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class GetPeerReply extends ProtoReply {

    public static final short REP_ID = 100;
    private final List<Host> peers;

    public GetPeerReply(List<Host> peers) {
        super(REP_ID);
        this.peers = peers;
    }


    public List<Host> peers() {
        return peers;
    }
}
