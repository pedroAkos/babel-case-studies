package consensus.utils;

import consensus.values.PaxosValue;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InstanceState {

    public final int iN;

    public SeqN highestAccept;
    public PaxosValue acceptedValue;

    private boolean decided;
    private final Set<Host> acceptedSet;
    public Map<SeqN, Set<Host>> prepareResponses;

    public InstanceState(int iN) {
        this.iN = iN;
        this.highestAccept = null;
        this.acceptedValue = null;
        this.decided = false;
        this.prepareResponses = new HashMap<>();
        this.acceptedSet = new HashSet<>();
    }

    public void accept(SeqN sN, PaxosValue value) {
        if(highestAccept == null || sN.greaterThan(highestAccept))
            acceptedSet.clear();
        highestAccept = sN;
        acceptedValue = value;
    }

    public int registerAccepted(SeqN sN, PaxosValue value, Host sender){
        accept(sN, value);
        acceptedSet.add(sender);
        return acceptedSet.size();
    }

    public  boolean isDecided() {
        return decided;
    }

    public void markDecided() {
        decided = true;
    }
}
