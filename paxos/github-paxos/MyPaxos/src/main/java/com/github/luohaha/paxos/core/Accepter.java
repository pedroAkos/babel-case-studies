package com.github.luohaha.paxos.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.luohaha.paxos.packet.*;
import com.github.luohaha.paxos.utils.ConfReader;
import com.github.luohaha.paxos.utils.FileUtils;
import com.github.luohaha.paxos.utils.client.CommClient;
import com.github.luohaha.paxos.utils.client.CommClientImpl;
import com.github.luohaha.paxos.utils.serializable.ObjectSerialize;
import com.github.luohaha.paxos.utils.serializable.ObjectSerializeImpl;
import com.github.luohaha.paxos.utils.server.CommServer;
import com.github.luohaha.paxos.utils.server.CommServerImpl;
import com.github.luohaha.paxos.utils.server.NonBlockServerImpl;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Accepter {
	static class Instance {
		// current ballot number
		private int ballot;
		// accepted value
		private Value value;
		// accepted value's ballot
		private int acceptedBallot;

		public Instance(int ballot, Value value, int acceptedBallot) {
			super();
			this.ballot = ballot;
			this.value = value;
			this.acceptedBallot = acceptedBallot;
		}

		public void setValue(Value value) {
			this.value = value;
		}
	}

	// accepter's state, contain each instances
	private Map<Integer, Instance> instanceState = new HashMap<>();
	// accepted value
	private Map<Integer, Value> acceptedValue = new HashMap<>();
	// accepter's id
	private transient int id;
	// proposers
	private transient List<InfoObject> proposers;
	// my conf
	private transient InfoObject my;
	// Save the last successfully submitted instance for optimization
	private volatile int lastInstanceId = 0;
	// Configuration file
	private ConfObject confObject;
	// Group id
	private int groupId;

	private Gson gson = new Gson();

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

	private Logger logger = Logger.getLogger("MyPaxos");
	//Client
	private CommClient client;

	private Learner learner;


	// Message queue, save packetbean
	private BlockingQueue<PacketBean> msgQueue = new LinkedBlockingQueue<>();

	public Accepter(int id, List<InfoObject> proposers, InfoObject my, ConfObject confObject, int groupId, CommClient client) {
		this.id = id;
		this.proposers = proposers;
		this.my = my;
		this.confObject = confObject;
		this.groupId = groupId;
		this.client = client;
		//instanceRecover();
		new Thread(() -> {
			while (true) {
				try {
					PacketBean msg = msgQueue.take();
					recvPacket(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * Insert packetbean into the message queue
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
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void recvPacket(PacketBean bean) throws UnknownHostException, IOException {
		switch (bean.getType()) {
		case "PreparePacket":
			//PreparePacket preparePacket = gson.fromJson(bean.getData(), PreparePacket.class);
			PreparePacket preparePacket = (PreparePacket) bean.getData();
			onPrepare(preparePacket.getPeerId(), preparePacket.getInstance(), preparePacket.getBallot());
			break;
		case "AcceptPacket":
			//System.out.println("Received accept packet");
			//AcceptPacket acceptPacket = gson.fromJson(bean.getData(), AcceptPacket.class);
			AcceptPacket acceptPacket = (AcceptPacket) bean.getData();
			onAccept(acceptPacket.getId(), acceptPacket.getInstance(), acceptPacket.getBallot(),
					acceptPacket.getValue());
			break;
		default:
			System.out.println("unknown type!!!");
			break;
		}
	}

	/**
	 * handle prepare from proposer
	 *
	 * @param instance
	 *            current instance
	 * @param ballot
	 *            prepare ballot
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void onPrepare(int peerId, int instance, int ballot) throws UnknownHostException, IOException {
		//System.out.println("Received prepare from " + peerId);
		if (!instanceState.containsKey(instance)) {
			instanceState.put(instance, new Instance(ballot, null, 0));
			// Persist to disk
			//instancePersistence();
			prepareResponse(peerId, id, instance, true, 0, null);
		} else {
			Instance current = instanceState.get(instance);
			if (ballot > current.ballot) {
				current.ballot = ballot;
				// Persist to disk
				//instancePersistence();
				prepareResponse(peerId, id, instance, true, current.acceptedBallot, current.value);
			} else {
				prepareResponse(peerId, id, instance, false, current.ballot, null);
			}
		}
	}

	/**
	 *
	 * @param id
	 *            accepter's id
	 * @param ok
	 *            ok or reject
	 * @param ab
	 *            accepted ballot
	 * @param av
	 *            accepted value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	private void prepareResponse(int peerId, int id, int instance, boolean ok, int ab, Value av)
			throws UnknownHostException, IOException {
		PacketBean bean = new PacketBean("PrepareResponsePacket",
				new PrepareResponsePacket(id, instance, ok, ab, av));
		InfoObject peer = getSpecInfoObect(peerId);
		this.client.sendTo(peer.getHost(), peer.getPort(),
				this.objectSerialize.objectToObjectArray(new Packet(bean, groupId, WorkerType.PROPOSER)));
	}

	/**
	 * handle accept from proposer
	 *
	 * @param instance
	 *            current instance
	 * @param ballot
	 *            accept ballot
	 * @param value
	 *            accept value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void onAccept(int peerId, int instance, int ballot, Value value) throws UnknownHostException, IOException {
		this.logger.info("[onaccept]" + peerId + " " + instance + " " + ballot + " " + value);
		if (!this.instanceState.containsKey(instance)) {
			acceptResponse(peerId, id, instance, false);
		} else {
			Instance current = this.instanceState.get(instance);
			if (ballot == current.ballot) {
				current.acceptedBallot = ballot;
				current.value = value;
				// success
				this.logger.info("[onaccept success]");
				this.acceptedValue.put(instance, value);
				if (!this.instanceState.containsKey(instance + 1)) {
					// Optimization in multi-paxos saves the prepare phase after continuous success
					this.instanceState.put(instance + 1, new Instance(1, null, 0));
				}
				// Save the location of the last successful instance for the proposer to execute directly from here
				this.lastInstanceId = instance;
				// Persist to disk
				//instancePersistence();
				//Added by pfouto
				//Trigger learner to inform other learners of accepted value
				PacketBean packetBean = new PacketBean("LearnRequest", new LearnRequest(id, instance));
				try {
					learner.sendPacket(packetBean);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//End added by pfouto
				acceptResponse(peerId, id, instance, true);
			} else {
				acceptResponse(peerId, id, instance, false);
			}
		}
		this.logger.info("[onaccept end]");
	}

	private void acceptResponse(int peerId, int id, int instance, boolean ok) throws UnknownHostException, IOException {
		InfoObject infoObject = getSpecInfoObect(peerId);
		PacketBean bean = new PacketBean("AcceptResponsePacket", new AcceptResponsePacket(id, instance, ok));
		this.client.sendTo(infoObject.getHost(), infoObject.getPort(),
				this.objectSerialize.objectToObjectArray(new Packet(bean, groupId, WorkerType.PROPOSER)));
	}

	/**
	 * The proposer gets the ID of the nearest instance from here
	 *
	 * @return
	 */
	public int getLastInstanceId() {
		return lastInstanceId;
	}

	/**
	 * Get specific info
	 *
	 * @param key
	 * @return
	 */
	private InfoObject getSpecInfoObect(int key) {
		for (InfoObject each : this.proposers) {
			if (key == each.getId()) {
				return each;
			}
		}
		return null;
	}

	/**
	 * Store instance on disk
	 */
	/*private void instancePersistence() {
		if (!this.confObject.isEnableDataPersistence())
			return;
		try {
			FileWriter fileWriter = new FileWriter(getInstanceFileAddr());
			fileWriter.write(gson.toJson(this.instanceState));
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	/**
	 * instance recovery
	 */
	/*private void instanceRecover() {
		if (!this.confObject.isEnableDataPersistence())
			return;
		String data = FileUtils.readFromFile(getInstanceFileAddr());
		if (data == null || data.length() == 0) {
			File file = new File(getInstanceFileAddr());
			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return;
		}
		this.instanceState.putAll(gson.fromJson(data, new TypeToken<Map<Integer, Instance>>() {
		}.getType()));
		this.instanceState.forEach((key, value) -> {
			if (value.value != null)
				this.acceptedValue.put(key, value.value);
		});
	}*/

	/**
	 * Get the persistent file location of the instance
	 * @return
	 */
	private String getInstanceFileAddr() {
		return this.confObject.getDataDir() + "accepter-" + this.groupId + "-" + this.id + ".json";
	}

	public Map<Integer, Value> getAcceptedValue() {
		return acceptedValue;
	}

	public Map<Integer, Instance> getInstanceState() {
		return instanceState;
	}

	public int getId() {
		return id;
	}

	public ConfObject getConfObject() {
		return confObject;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setLearner(Learner learner) {
		this.learner = learner;
	}
}
