package membership.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class GetPeerRequest extends ProtoRequest {

    public static final short REQ_ID = 100;

    private final int nPeers;

    public GetPeerRequest(int nPeers) {
        super(REQ_ID);
        this.nPeers = nPeers;
    }

    public int howMany() {
        return nPeers;
    }
}
