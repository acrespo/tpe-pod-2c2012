package ar.edu.itba.pod.legajo50758.impl.utils;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.jgroups.Address;

import ar.edu.itba.pod.legajo50758.api.Signal;

public class Utils {

	public static Tuple<Address, Address> chooseRandomMember(List<Address> members) {
		
		if (members.size() == 1) {
			return new Tuple<Address, Address>(members.get(0), members.get(0));
		} else {
			int chosen = (int) (Math.random() * members.size());
			int i = 0;
			Address primaryCopyAddress = null;
			Address backupAddress = null;
			for (Address address : members) {
					if (i == chosen) {
						primaryCopyAddress = address;
					} else if ((chosen + 1) % members.size()  == i) {
						backupAddress = address;
					}
					i++;
			}

			return new Tuple<Address, Address>(primaryCopyAddress, backupAddress);
		}
	}
	
	public static void log(String format, Object[] params) {
		System.out.println(String.format(format, params));
	}
	
	public static SignalInfo searchForSignal(Signal obj, Collection<BlockingQueue<SignalInfo>> collection) {

		for (BlockingQueue<SignalInfo> list : collection) {
			for (SignalInfo sInfo : list) {
				if (sInfo.getSignal().equals(obj)) {
					return sInfo;
				}
			}
		}
		return null;
	}

	public static Queue<SignalInfo> searchForSignal(Address nodeDown, Collection<BlockingQueue<SignalInfo>> collection) {
		
		Queue<SignalInfo> result = new LinkedList<>(); 
		for (BlockingQueue<SignalInfo> list : collection) {
			for (SignalInfo sInfo : list) {
				if (sInfo.getCopyAddress().equals(nodeDown)) {
					result.add(sInfo);
				}
			}
		}	
		return result;
		
	}
	
	public static void nodeSnapshot(Address nodeAddress, SignalInfoMultimap<Integer> map,
			SignalInfoMultimap<Address> replicas) {
		
		
		System.out.println("------------------------------");
		System.out.println("I am:" + nodeAddress);
		System.out.println("These are my replicas:");
		for (BlockingQueue<SignalInfo> q: replicas.values()) {
			for (SignalInfo s: q) {
				System.out.println(s.getCopyAddress());
			}
			
		}
		System.out.println("These are my primaries:");
		for (BlockingQueue<SignalInfo> q: map.values()) {
			for (SignalInfo s: q) {
				System.out.println(s.getCopyAddress());
			}
			
		}
		System.out.println("------------------------------");
	}
	
	public static void printSignals(Iterable<SignalInfo> collection, String msg) {
		
		List<Address> list = new LinkedList<>();
		for (SignalInfo s: collection) {
			list.add(s.getCopyAddress());
		}
		System.out.println(msg + list);
	}
	
	public static void waitForResponses(Iterable<Future<Object>> futureResponses) {
		for (final Future<Object> future : futureResponses) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
}
