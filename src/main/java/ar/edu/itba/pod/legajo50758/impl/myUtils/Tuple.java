package ar.edu.itba.pod.legajo50758.impl.myUtils;

import java.io.Serializable;

public class Tuple<X, Y> implements Serializable {
	
	private static final long serialVersionUID = -3026846793021292134L;
	
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
