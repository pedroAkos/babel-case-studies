package membership;

import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static membership.PeerSampling.*;

public class View {

    private final Random rnd;
    private final PeerSelection peerSelection;
    private final Host self;
    private LinkedHashMap<Host, NodeDescriptor> nodes;

    public View(PeerSelection peerSelection, Host self) {
        nodes = new LinkedHashMap<>();
        this.peerSelection = peerSelection;
        this.rnd = new Random();
        this.self = self;
    }

    public View(PeerSelection peerSelection, Host self, Host contactHost) {
        this(peerSelection, self);
        nodes.put(contactHost, new NodeDescriptor());
    }

    @Override
    public String toString() {
        return "View{" +
                "nodes=" + nodes +
                ", peerSelection=" + peerSelection +
                '}';
    }

    public Host selectPeer() {
        Stream<Map.Entry<Host, NodeDescriptor>> live = nodes.entrySet().stream().filter(n -> n.getValue().connected);
        if (peerSelection == PeerSelection.RAND) {
            List<Host> liveList = live.map(Map.Entry::getKey).collect(Collectors.toList());
            return liveList.size() == 0 ? null : liveList.get(rnd.nextInt(liveList.size()));
        } else if (peerSelection == PeerSelection.TAIL)
            return live.reduce((e1, e2) -> e1.getValue().age > e2.getValue().age ? e1 : e2)
                    .map(Map.Entry::getKey).orElse(null);
        else
            throw new AssertionError();
    }

    public void permute() {
        List<Host> list = new ArrayList<>(nodes.keySet());
        Collections.shuffle(list);
        LinkedHashMap<Host, NodeDescriptor> shuffledMap = new LinkedHashMap<>();
        list.forEach(k -> shuffledMap.put(k, nodes.get(k)));
        nodes = shuffledMap;
    }

    public void moveOldestToEnd(int H) {
        List<Host> oldest = nodes.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<Host, NodeDescriptor> o) -> o.getValue().age).reversed())
                .map(Map.Entry::getKey).limit(H).collect(Collectors.toList());
        for (Host node : oldest) {
            NodeDescriptor remove = nodes.remove(node);
            nodes.put(node, remove);
        }
    }

    public List<Pair<Host, Integer>> head(int nElements) {
        List<Pair<Host, Integer>> ret = new LinkedList<>();
        Iterator<Map.Entry<Host, NodeDescriptor>> iterator = nodes.entrySet().iterator();
        while (iterator.hasNext() && ret.size() < nElements) {
            Map.Entry<Host, NodeDescriptor> next = iterator.next();
            ret.add(Pair.of(next.getKey(), next.getValue().age));
        }
        return ret;
    }

    public NodeDescriptor getNodeDescriptor(Host host) {
        return nodes.get(host);
    }

    public Pair<List<Host>, List<Host>> select(int c, int h, int s, List<Pair<Host, Integer>> buffer) {
        List<Host> added = append(buffer);
        List<Host> removed = new LinkedList<>();
        removed.addAll(removeOldItems(Math.min(h, nodes.size() - c)));
        removed.addAll(removeHead(Math.min(s, nodes.size() - c)));
        removed.addAll(removeAtRandom(nodes.size() - c));
        return Pair.of(added, removed);
    }

    private List<Host> removeAtRandom(int nItems) {
        if (nItems <= 0) return Collections.emptyList();
        List<Host> removed = new LinkedList<>();
        List<Host> shuffledKeys = new LinkedList<>(nodes.keySet());
        Collections.shuffle(shuffledKeys, rnd);
        for (int i = 0; i < nItems; i++) {
            Host toRemove = shuffledKeys.remove(0);
            nodes.remove(toRemove);
            removed.add(toRemove);
        }
        return removed;
    }

    private List<Host> removeHead(int nItems) {
        if (nItems <= 0) return Collections.emptyList();
        Iterator<Host> iterator = nodes.keySet().iterator();
        List<Host> removed = new LinkedList<>();
        for (int i = 0; i < nItems; i++) {
            Host h = iterator.next();
            iterator.remove();
            removed.add(h);
        }
        return removed;
    }

    private List<Host> removeOldItems(int nItems) {
        if (nItems <= 0) return Collections.emptyList();
        List<Host> toRemove = nodes.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<Host, NodeDescriptor> e) -> e.getValue().age).reversed())
                .limit(nItems).map(Map.Entry::getKey).collect(Collectors.toList());
        toRemove.forEach(k -> nodes.remove(k));
        return toRemove;
    }

    private List<Host> append(List<Pair<Host, Integer>> buffer) {
        List<Host> added = new LinkedList<>();
        for (Pair<Host, Integer> p : buffer) {
            if (p.getKey().equals(self)) continue;
            NodeDescriptor nodeDescriptor = nodes.get(p.getKey());
            if (nodeDescriptor != null)
                nodeDescriptor.age = Math.min(nodeDescriptor.age, p.getValue());
            else {
                nodes.put(p.getKey(), new NodeDescriptor(p.getRight()));
                added.add(p.getKey());
            }
        }
        return added;
    }


    public void increaseAge() {
        nodes.values().forEach(d -> d.age++);
    }

    public List<Host> getPeer(int howMany) {
        Stream<Map.Entry<Host, NodeDescriptor>> live = nodes.entrySet().stream().filter(n -> n.getValue().connected);
        List<Host> liveList = live.map(Map.Entry::getKey).collect(Collectors.toList());
        if(liveList.size() > howMany) {
            List<Host> ret = new ArrayList<>(howMany);
            while (ret.size() < howMany && !liveList.isEmpty()) {
                ret.add(liveList.remove(rnd.nextInt(liveList.size())));
            }
            return ret;
        } else {
            return liveList;
        }
    }

    public static class NodeDescriptor {
        public boolean connected;
        public int age;

        public NodeDescriptor() {
            this(0);
        }

        public NodeDescriptor(Integer value) {
            connected = false;
            age = value;
        }

        @Override
        public String toString() {
            return "{" +
                    "age=" + age +
                    ", " + (connected ? "connected" : "DISconnected") +
                    '}';
        }
    }
}
