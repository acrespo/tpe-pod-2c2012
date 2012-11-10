package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;

import org.jgroups.Address;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class SignalInfo implements Serializable {

	private static final long serialVersionUID = -2878451377274694626L;
	
	private final Signal signal;
	private Address copyAddress;
	private final boolean isPrimary;
	
	public SignalInfo(Signal signal, Address copyAddress, boolean isPrimary) {
		this.signal = signal;
		this.copyAddress = copyAddress;
		this.isPrimary = isPrimary;
	}
	
	
	public Address getCopyAddress() {
		return copyAddress;
	}
	 
	public Signal getSignal() {
		return signal;
	}
	
	public boolean isPrimary() {
		return isPrimary;
	}
	
	public void setCopyAddress(Address copyAddress) {
		this.copyAddress = copyAddress;
	}
}
