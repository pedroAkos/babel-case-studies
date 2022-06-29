package membership.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PushTimer extends ProtoTimer {

    public static final short TIMER_ID = 101;

    public PushTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
