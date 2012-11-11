package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class RequestMessage implements Serializable {

	private static final long serialVersionUID = 5467354726106264042L;
	
	private final Serializable content;
	private final int id;

	public RequestMessage(int id, Serializable obj) {
		this.id = id;
		this.content = obj;
	}

	public int getId() {
		return id;
	}

	public Serializable getContent() {
		return content;
	}
}
