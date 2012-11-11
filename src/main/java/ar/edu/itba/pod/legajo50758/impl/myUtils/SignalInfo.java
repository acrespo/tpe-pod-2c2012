package ar.edu.itba.pod.legajo50758.impl.myUtils;

import java.io.Serializable;

import net.jcip.annotations.ThreadSafe;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import ar.edu.itba.pod.legajo50758.api.Signal;

@ThreadSafe
public class SignalInfo implements Serializable {

	private static final long serialVersionUID = -2878451377274694626L;
	
	private final Signal signal;
	private final boolean isPrimary;
	private Object lock = new Object();
	@GuardedBy("lock") private Address copyAddress;
	
	public SignalInfo(Signal signal, Address copyAddress, boolean isPrimary) {
		this.signal = signal;
		this.copyAddress = copyAddress;
		this.isPrimary = isPrimary;
	}
	
	
	public Address getCopyAddress() {
		synchronized (lock) {			
			return copyAddress;
		}
	}
	 
	public Signal getSignal() {
		return signal;
	}
	
	public boolean isPrimary() {
		return isPrimary;
	}
	
	public void setCopyAddress(Address copyAddress) {
		synchronized (copyAddress) {
			this.copyAddress = copyAddress;			
		}
	}

	
	/**
	 * 
	 * Used only in a special case. Its fine that its based only in the signal and isPrimary.
	 * 
	 */
	@Override
	public boolean equals(Object obj) {
		
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SignalInfo other = (SignalInfo) obj;
		if (isPrimary != other.isPrimary)
			return false;
		if (signal == null) {
			if (other.signal != null)
				return false;
		} else if (!signal.equals(other.signal))
			return false;
		return true;
	}
}
