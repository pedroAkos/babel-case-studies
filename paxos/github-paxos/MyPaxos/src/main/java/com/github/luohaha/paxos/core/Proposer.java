package com.github.luohaha.paxos.core;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.logging.Logger;

import com.github.luohaha.paxos.packet.AcceptPacket;
import com.github.luohaha.paxos.packet.AcceptResponsePacket;
import com.github.luohaha.paxos.packet.Packet;
import com.github.luohaha.paxos.packet.PacketBean;
import com.github.luohaha.paxos.packet.PreparePacket;
import com.github.luohaha.paxos.packet.PrepareResponsePacket;
import com.github.luohaha.paxos.packet.Value;
import com.github.luohaha.paxos.utils.client.CommClient;
import com.github.luohaha.paxos.utils.client.CommClientImpl;
import com.github.luohaha.paxos.utils.serializable.ObjectSerialize;
import com.github.luohaha.paxos.utils.serializable.ObjectSerializeImpl;
import com.github.luohaha.paxos.utils.server.CommServer;
import com.github.luohaha.paxos.utils.server.CommServerImpl;
import com.github.luohaha.paxos.utils.server.NonBlockServerImpl;
import com.google.gson.Gson;

public class Proposer {
	enum Proposer_State {
		READY, PREPARE, ACCEPT, FINISH
	}

	class Instance {
		private int ballot;
		// a set for promise receive
		private Set<Integer> pSet;
		// value found after phase 1
		private Value value;
		// value's ballot
		private int valueBallot;
		// accept set
		private Set<Integer> acceptSet;
		// is wantvalue doneValue
		private boolean isSucc;
		// state
		private Proposer_State state;

		public Instance(int ballot, Set<Integer> pSet, Value value, int valueBallot, Set<Integer> acceptSet,
				boolean isSucc, Proposer_State state) {
			super();
			this.ballot = ballot;
			this.pSet = pSet;
			this.value = value;
			this.valueBallot = valueBallot;
			this.acceptSet = acceptSet;
			this.isSucc = isSucc;
			this.state = state;
		}

	}

	private Map<Integer, Instance> instanceState = new ConcurrentHashMap<>();

	// current instance
	private int currentInstance = 0;

	// proposer's id
	private int id;

	// accepter's number
	private int accepterNum;

	// accepters
	private List<InfoObject> accepters;

	// my info
	private InfoObject my;

	// timeout for each phase(ms)
	private int timeout;

	// Ready to submit status
	private BlockingQueue<Value> readyToSubmitQueue = new ArrayBlockingQueue<>(1);

	// Status of successful submission
	private BlockingQueue<Value> hasSummitQueue = new ArrayBlockingQueue<>(1);

	// Whether the last submission was successful
	private boolean isLastSumbitSucc = false;

	// The accepter of this node
	private Accepter accepter;

	// Group id
	private int groupId;

	// Message queue, save packetbean
	private BlockingQueue<PacketBean> msgQueue = new LinkedBlockingQueue<>();

	private BlockingQueue<PacketBean> submitMsgQueue = new LinkedBlockingQueue<>();

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

	private Logger logger = Logger.getLogger("MyPaxos");

	// Client
	private CommClient client;

	public Proposer(int id, List<InfoObject> accepters, InfoObject my, int timeout, Accepter accepter, int groupId,
			CommClient client) {
		this.id = id;
		this.accepters = accepters;
		this.accepterNum = accepters.size();
		this.my = my;
		this.timeout = timeout;
		this.accepter = accepter;
		this.groupId = groupId;
		this.client = client;
		new Thread(() -> {
			while (true) {
				try {
					PacketBean msg = this.msgQueue.take();
					recvPacket(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
		new Thread(() -> {
			while (true) {
				try {
					PacketBean msg = this.submitMsgQueue.take();
					submit((Value) msg.getData());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * Insert packetbean into the message queue
	 *
	 * @param bean
	 * @throws InterruptedException
	 */
	public void sendPacket(PacketBean bean) throws InterruptedException {
		this.msgQueue.put(bean);
	}

	/**
	 * Process the received packetbean
	 *
	 * @param bean
	 * @throws InterruptedException
	 */
	public void recvPacket(PacketBean bean) throws InterruptedException {
		switch (bean.getType()) {
		case "PrepareResponsePacket":
			PrepareResponsePacket prepareResponsePacket = (PrepareResponsePacket) bean.getData();
			onPrepareResponse(prepareResponsePacket.getId(), prepareResponsePacket.getInstance(),
					prepareResponsePacket.isOk(), prepareResponsePacket.getAb(), prepareResponsePacket.getAv());
			break;
		case "AcceptResponsePacket":
			//System.out.println("Received accept response");
			AcceptResponsePacket acceptResponsePacket = (AcceptResponsePacket) bean.getData();
			onAcceptResponce(acceptResponsePacket.getId(), acceptResponsePacket.getInstance(),
					acceptResponsePacket.isOk());
			break;
		case "SubmitPacket":
			this.submitMsgQueue.add(bean);
			break;
		default:
			System.out.println("unknown type!!!");
			break;
		}
	}

	/**
	 * The client submits the status that it wants to submit to the proposer
	 *
	 * @param object
	 * @return
	 * @throws InterruptedException
	 */
	public Value submit(Value object) throws InterruptedException {
		this.readyToSubmitQueue.put(object);
		beforPrepare();
		Value value = this.hasSummitQueue.take();
		return value;
	}

	/**
	 *
	 * Before prepare
	 */
	public void beforPrepare() {
		// Get the last instance id of the accepter
		this.currentInstance = Math.max(this.currentInstance, accepter.getLastInstanceId());
		this.currentInstance++;
		Instance instance = new Instance(1, new HashSet<>(), null, 0, new HashSet<>(), false, Proposer_State.READY);
		this.instanceState.put(this.currentInstance, instance);
		if (this.isLastSumbitSucc == false) {
			// Execute the complete process
			prepare(this.id, this.currentInstance, 1);
		} else {
			// Optimization in multi-paxos, accept directly
			instance.isSucc = true;
			accept(this.id, this.currentInstance, 1, this.readyToSubmitQueue.peek());
		}
	}

	/**
	 * Send prepare to all accepters and set a timeout. If it is timed out, it is judged whether phase 1 is completed, if not, then the ballot increments by one and then continues to execute phase one.
	 *
	 * @param instance
	 *            current instance
	 * @param ballot
	 *            prepare's ballot
	 */
	private void prepare(int id, int instance, int ballot) {
		this.instanceState.get(instance).state = Proposer_State.PREPARE;
		try {
			PacketBean bean = new PacketBean("PreparePacket", new PreparePacket(id, instance, ballot));
			byte[] msg = this.objectSerialize.objectToObjectArray(new Packet(bean, groupId, WorkerType.ACCEPTER));
			//System.out.println(accepters);
			this.accepters.forEach((info) -> {
				try {
					//System.out.println("Sending prepare to: " + info.getHost() + " " + info.getPort());
					this.client.sendTo(info.getHost(), info.getPort(), msg);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		setTimeout(new TimerTask() {

			@Override
			public void run() {
				// retry phase 1 again!
				Instance current = instanceState.get(instance);
				if (current.state == Proposer_State.PREPARE) {
					System.out.println("retry phase 1 for instance " + instance);
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
	}

	/**
	 * Receive the accepter's reply to prepare
	 *
	 * @param peerId
	 * @param instance
	 * @param ok
	 * @param ab
	 * @param av
	 * @throws InterruptedException
	 */
	public void onPrepareResponse(int peerId, int instance, boolean ok, int ab, Value av) {
		Instance current = this.instanceState.get(instance);
		if (current.state != Proposer_State.PREPARE)
			return;
		if (ok) {
			current.pSet.add(peerId);
			if (ab > current.valueBallot && av != null) {
				current.valueBallot = ab;
				current.value = av;
				current.isSucc = false;
			}
			if (current.pSet.size() >= this.accepterNum / 2 + 1) {
				if (current.value == null) {
					Value object = this.readyToSubmitQueue.peek();
					current.value = object;
					current.isSucc = true;
				}
				accept(id, instance, current.ballot, current.value);
			}
		}
	}

	/**
	 * Send accept to all accepters and set the status.
	 *
	 * @param id
	 * @param instance
	 * @param ballot
	 * @param value
	 */
	private void accept(int id, int instance, int ballot, Value value) {
		this.instanceState.get(instance).state = Proposer_State.ACCEPT;
		try {
			PacketBean bean = new PacketBean("AcceptPacket", new AcceptPacket(id, instance, ballot, value));
			byte[] msg = this.objectSerialize.objectToObjectArray(new Packet(bean, groupId, WorkerType.ACCEPTER));
			//System.out.println("Sending accepts to " + accepters);
			this.accepters.forEach((info) -> {
				try {
					this.client.sendTo(info.getHost(), info.getPort(), msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/*
		setTimeout(new TimerTask() {
			@Override
			public void run() {
				// retry phase 2 again!
				Instance current = instanceState.get(instance);
				if (current.state == Proposer_State.ACCEPT) {
					System.out.println("retry phase 2 for instance " + instance);
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
		*/
	}

	/**
	 * Receive the accept response returned by the accepter
	 *
	 * @param peerId
	 * @param instance
	 * @param ok
	 * @throws InterruptedException
	 */
	public void onAcceptResponce(int peerId, int instance, boolean ok) throws InterruptedException {
		Instance current = this.instanceState.get(instance);
		if (current.state != Proposer_State.ACCEPT)
			return;
		if (ok) {
			current.acceptSet.add(peerId);
			if (current.acceptSet.size() >= this.accepterNum / 2 + 1) {
				// End of process
				done(instance);
				if (current.isSucc) {
					this.isLastSumbitSucc = true;
					this.hasSummitQueue.put(this.readyToSubmitQueue.take());
				} else {
					// Explain that the id of this instance is already occupied
					this.isLastSumbitSucc = false;
					beforPrepare();
				}
			}
		}
	}

	/**
	 * The paxos election is over
	 */
	public void done(int instance) {
		this.instanceState.get(instance).state = Proposer_State.FINISH;
	}

	/**
	 * set timeout task
	 *
	 * @param task
	 */
	private void setTimeout(TimerTask task) {
		new Timer().schedule(task, this.timeout);
	}
}
