package com.github.luohaha.paxos.log;

import java.io.IOException;

import com.github.luohaha.paxos.core.Accepter;

public interface MyPaxosLog {
	/**
	 * Get the location of the log file
	 * @return
	 * Return file location
	 */
	public String getLogFileAddr();
	/**
	 * Recover from log file
	 */
	public void recoverFromLog() throws IOException;
	/**
	 * Every time you add or modify the ballot, this function will be called to add the record to the log
	 * @param instanceId
	 * @param ballot
	 */
	public void setInstanceBallot(int instanceId, int ballot) throws IOException;
	/**
	 * Every time you add or modify acceptedBallot, this function will be called to add the record to the log
	 * @param instanceId
	 * @param acceptedBallot
	 */
	public void setInstanceAcceptedBallot(int instanceId, int acceptedBallot) throws IOException;
	/**
	 * This function will be called every time a new addition is added and the value is modified, and the record will be added to the log
	 * @param instanceId
	 * @param value
	 */
	public void setInstanceValue(int instanceId, Object value) throws IOException;
	/**
	 * Clear log
	 * @throws IOException
	 */
	public void clearLog() throws IOException;
}
