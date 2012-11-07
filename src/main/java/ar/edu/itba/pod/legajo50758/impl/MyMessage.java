package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;

import org.jgroups.Address;

public class MyMessage<T> implements Serializable {

	private final T content;
	private final Operation op;
	private Boolean replica;
	private Address copyAddress;
	
	public MyMessage(T content, Operation op) {
		this.content = content;
		this.op = op;
	}
	
	public MyMessage(T content, Operation op, boolean replica, Address copyAddress) {
		this(content, op);
		this.replica = replica;
		this.copyAddress = copyAddress;
	}
	
	
	public T getContent() {
		return content;
	}
	
	public Operation getOp() {
		return op;
	}

	public Boolean isReplica() {
		return replica;
	}

	public Address getCopyAddress() {
		return copyAddress;
	}
}