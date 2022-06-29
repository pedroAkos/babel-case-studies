package com.github.luohaha.paxos.core;

public interface PaxosCallback {
	/**
	 * Actuator, used to execute a certain state
	 * @param msg
	 */
	public void callback(byte[] msg);
}
