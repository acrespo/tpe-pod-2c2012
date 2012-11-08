package ar.edu.itba.pod.legajo50758.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class MyReceiverAdapter extends ReceiverAdapter {

	private final JChannel channel;
	private final BlockingQueue<Message> msgQueue;
	private View currentView = null;

	public MyReceiverAdapter(JChannel channel, BlockingQueue<Message> msgQueue) {
		this.channel = channel;
		this.msgQueue = msgQueue;
	}

	@Override
	public void receive(Message message) {
		
//		Utils.log("Received: %s: %s", new Object[] { message.getObject(), message.getSrc() });
//		if (message.getObject() instanceof MyMessage<?>) {
			msgQueue.add(message);
//		}
	}
	
	@Override
	public void viewAccepted(View newView) {
		
		System.out.println("TOPOLOGY CHANGE!!!");
		if (currentView == null) {
			currentView = newView;
			return;
		}
		System.out.println("Former members: " + currentView.getMembers());
		System.out.println("New members: " + newView.getMembers());
		
		for (Address member: newView.getMembers()) {
			if (!currentView.getMembers().contains(member)) {
				// member is NEW NODE
				Tuple<Address, List<Address>> tuple = new Tuple<>(member, newView.getMembers());
				MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEUP);
				msgQueue.add(new Message(channel.getAddress(), myMsg));
				currentView = newView;
				System.out.println("NOW our members are :" + currentView.getMembers());
				return;
			}
		}
		
		for (Address currMember: currentView.getMembers()) {
			if (!newView.getMembers().contains(currMember)) {
				//TODO currMember is DOWN
				System.out.println("NODE IS DOWN");
				Tuple<Address, List<Address>> tuple = new Tuple<>(currMember, newView.getMembers());
				MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEDOWN);
				msgQueue.add(new Message(channel.getAddress(), myMsg));
				currentView = newView;
				System.out.println("NOW our members are :" + currentView.getMembers());
				return;
			}
		}

	}
}
