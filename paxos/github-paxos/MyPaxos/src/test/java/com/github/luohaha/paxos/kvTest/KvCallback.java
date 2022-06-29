package com.github.luohaha.paxos.kvTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.luohaha.paxos.core.PaxosCallback;
import com.github.luohaha.paxos.utils.client.CommClient;
import com.github.luohaha.paxos.utils.client.CommClientImpl;
import com.github.luohaha.paxos.utils.server.CommServer;
import com.github.luohaha.paxos.utils.server.CommServerImpl;
import com.google.gson.Gson;

public class KvCallback implements PaxosCallback {
	/**
	 * Use map to save key and value mapping
	 */
	private Map<String, String> kv = new HashMap<>();
	private Gson gson = new Gson();

	@Override
	public void callback(byte[] msg) {
		/**
		 * A total of three actions are provided: get: get put: add delete: delete
		 */
		String msString =  new String(msg);
		MsgBean bean = gson.fromJson(String.valueOf(msString), MsgBean.class);
		switch (bean.getType()) {
		case "get":
			System.out.println(kv.get(bean.getKey()));
			break;
		case "put":
			kv.put(bean.getKey(), bean.getValue());
			System.out.println("ok");
			break;
		case "delete":
			kv.remove(bean.getKey());
			System.out.println("ok");
			break;
		default:
			break;
		}
	}

}
