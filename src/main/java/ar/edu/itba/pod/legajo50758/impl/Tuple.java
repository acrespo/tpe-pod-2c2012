package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;

public class Tuple<X, Y> implements Serializable {
	private final X x;
	private final Y y;

	public Tuple(X x, Y y) {
		this.x = x;
		this.y = y;
	}
	
	public X getFirst() {
		return x;
	}
	
	public Y getSecond() {
		return y;
	}
}
