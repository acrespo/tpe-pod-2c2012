package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;

public class ResponseMessage implements Serializable {

	private static final long serialVersionUID = -3729304125719629253L;
	
	private final Serializable content;
	private final int id;

	public ResponseMessage(int id, Serializable obj) {
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
