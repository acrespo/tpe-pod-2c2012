package ar.edu.itba.pod.legajo50758.message;

import java.io.Serializable;

import net.jcip.annotations.ThreadSafe;

import org.jgroups.Address;

@ThreadSafe
public class MyMessage<T> implements Serializable {

	private static final long serialVersionUID = 7048178676105024181L;
	
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
