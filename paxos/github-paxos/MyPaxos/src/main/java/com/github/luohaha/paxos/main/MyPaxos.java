package com.github.luohaha.paxos.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sound.midi.MidiDevice.Info;

import com.github.luohaha.paxos.core.Accepter;
import com.github.luohaha.paxos.core.ConfObject;
import com.github.luohaha.paxos.core.InfoObject;
import com.github.luohaha.paxos.core.Learner;
import com.github.luohaha.paxos.core.PaxosCallback;
import com.github.luohaha.paxos.core.Proposer;
import com.github.luohaha.paxos.packet.Packet;
import com.github.luohaha.paxos.utils.ConfReader;
import com.github.luohaha.paxos.utils.FileUtils;
import com.github.luohaha.paxos.utils.client.ClientImplByLC4J;
import com.github.luohaha.paxos.utils.client.CommClient;
import com.github.luohaha.paxos.utils.client.NonBlockClientImpl;
import com.github.luohaha.paxos.utils.serializable.ObjectSerialize;
import com.github.luohaha.paxos.utils.serializable.ObjectSerializeImpl;
import com.github.luohaha.paxos.utils.server.CommServer;
import com.github.luohaha.paxos.utils.server.CommServerImpl;
import com.github.luohaha.paxos.utils.server.NonBlockServerImpl;
import com.github.luohaha.paxos.utils.server.ServerImplByLC4J;
import com.google.gson.Gson;

public class MyPaxos {

	/**
	 * Global profile information
	 */
	private ConfObject confObject;

	/**
	 * Information of this node
	 */
	private InfoObject infoObject;

	/**
	 * The location of the configuration file
	 */
	private String confFile;

	private Map<Integer, PaxosCallback> groupidToCallback = new HashMap<>();

	private Map<Integer, Proposer> groupidToProposer = new HashMap<>();

	private Map<Integer, Accepter> groupidToAccepter = new HashMap<>();

	private Map<Integer, Learner> groupidToLearner = new HashMap<>();

	private Gson gson = new Gson();

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

	private Logger logger = Logger.getLogger("MyPaxos");

	/*
	 * Client
	 */
	private CommClient client;

	public MyPaxos(String confFile) throws IOException {
		super();
		this.confFile = confFile;
		this.confObject = gson.fromJson(FileUtils.readFromFile(this.confFile), ConfObject.class);
		this.infoObject = getMy(this.confObject.getNodes());
		// Start the client
		this.client = new ClientImplByLC4J(4);
		this.logger.setLevel(Level.WARNING);
	}

	public MyPaxos(ConfObject confObject) throws IOException {
			Logger.getLogger("LightComm4J").setLevel(Level.WARNING);
			this.confObject = confObject;
			this.infoObject = getMy(this.confObject.getNodes());
			// Start the client
			this.client = new ClientImplByLC4J(4);
			this.logger.setLevel(Level.WARNING);
	}

	/**
	 * Set log level
	 * @param level
	 * level
	 */
	public void setLogLevel(Level level) {
		this.logger.setLevel(level);
	}

	/**
	 * add handler
	 * @param handler
	 * handler
	 */
	public void addLogHandler(Handler handler) {
		this.logger.addHandler(handler);
	}

	/**
	 *
	 * @param groupId
	 * @param executor
	 */
	public Proposer setGroupId(int groupId, PaxosCallback executor) {
		Accepter accepter = new Accepter(infoObject.getId(), confObject.getNodes(), infoObject, confObject, groupId,
				this.client);
		Proposer proposer = new Proposer(infoObject.getId(), confObject.getNodes(), infoObject, confObject.getTimeout(),
				accepter, groupId, this.client);
		Learner learner = new Learner(infoObject.getId(), confObject.getNodes(), infoObject, confObject, accepter,
				executor, groupId, this.client);
		accepter.setLearner(learner);
		this.groupidToCallback.put(groupId, executor);
		this.groupidToAccepter.put(groupId, accepter);
		this.groupidToProposer.put(groupId, proposer);
		this.groupidToLearner.put(groupId, learner);
		return proposer;
	}

	/**
	 * Get my accepter or proposer information
	 *
	 * @param infoObjects
	 * @return
	 */
	private InfoObject getMy(List<InfoObject> infoObjects) {
		for (InfoObject each : infoObjects) {
			if (each.getId() == confObject.getMyid()) {
				return each;
			}
		}
		return null;
	}

	/**
	 * Start the paxos server
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void start() throws IOException, InterruptedException, ClassNotFoundException {
		// Start the paxos server
		CommServer server = new ServerImplByLC4J(this.infoObject.getHost(), this.infoObject.getPort(), 4);
		System.out.println("paxos server-" + confObject.getMyid() + " start... " + this.infoObject);
		while (true) {
			byte[] data = server.recvFrom();
			//Packet packet = gson.fromJson(new String(data), Packet.class);
			Packet packet = objectSerialize.byteArrayToObject(data, Packet.class);
			int groupId = packet.getGroupId();
			Accepter accepter = this.groupidToAccepter.get(groupId);
			Proposer proposer = this.groupidToProposer.get(groupId);
			Learner learner = this.groupidToLearner.get(groupId);
			if (accepter == null || proposer == null || learner == null) {
				return;
			}
			switch (packet.getWorkerType()) {
			case ACCEPTER:
				accepter.sendPacket(packet.getPacketBean());
				break;
			case PROPOSER:
				proposer.sendPacket(packet.getPacketBean());
				break;
			case LEARNER:
				learner.sendPacket(packet.getPacketBean());
				break;
			case SERVER:
				proposer.sendPacket(packet.getPacketBean());
				break;
			default:
				break;
			}
		}
	}
}
