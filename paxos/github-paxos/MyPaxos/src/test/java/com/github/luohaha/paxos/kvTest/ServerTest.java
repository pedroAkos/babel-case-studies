package com.github.luohaha.paxos.kvTest;

import java.io.IOException;

import com.github.luohaha.paxos.main.MyPaxos;

public class ServerTest {
	public static void main(String[] args) {
		try {
			MyPaxos server = new MyPaxos("./deploy/mypaxos/conf/conf1.json");
			server.setGroupId(1, new KvCallback());
			server.setGroupId(2, new KvCallback());
			server.start();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
