package dissemination.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class BroadcastRequest extends ProtoRequest {

    public static final short REQUEST_ID = 301;

    private final byte[] msg;

    public BroadcastRequest(byte[] msg) {
        super(REQUEST_ID);
        this.msg = msg;
    }

    public byte[] getMsg() {
        return msg;
    }
}
