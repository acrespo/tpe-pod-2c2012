package ar.edu.itba.pod.legajo50758.impl.utils;

import java.util.concurrent.ExecutionException;

public class DegradedModeException extends ExecutionException {

	public DegradedModeException(String string, Exception e) {
		super(string, e);
	}

	public DegradedModeException(String string) {
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
