package ar.edu.itba.pod.legajo50758.impl;

import java.util.List;

import org.jgroups.Address;

public class Utils {

	public static Tuple<Address, Address> chooseRandomMember(List<Address> members) {
		
		if (members.size() == 1) {
			return new Tuple<Address, Address>(members.get(0), members.get(0));
		} else {
			int chosen = (int) (Math.random() * members.size());
			System.out.println("chosen =" + chosen);
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
	
}
