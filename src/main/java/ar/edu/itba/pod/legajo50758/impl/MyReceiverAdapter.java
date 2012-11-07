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
		
		Utils.log("Received: %s: %s", new Object[] { message.getObject(), ((MyMessage<?>) message.getObject()).getOp(), message.getSrc() });
//		if (message.getObject() instanceof MyMessage<?>) {
			msgQueue.add(message);
//		}
	}
	
	@Override
	public void viewAccepted(View view) {
		System.out.println("NODE UP");
//		LA PARECER NO SE PUEDE poner una view dentro de un message
//		msgQueue.add(new Message(channel.getAddress(), new MyMessage<View>(view, Operation.MEMBERSHIPCHANGED)));
		
		if (currentView == null) {
			currentView = view;
			return;
		}
		
		for (Address member: view.getMembers()) {
			if (!currentView.getMembers().contains(member)) {
				// member is NEW NODE
				Tuple<Address, List<Address>> tuple = new Tuple<>(member, view.getMembers());
				MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEUP);
				msgQueue.add(new Message(channel.getAddress(), myMsg));
				return;
			}
		}
		for (Address currMember: currentView.getMembers()) {
			if (!view.getMembers().contains(currMember)) {
				//TODO currMember is DOWN
				Tuple<Address, List<Address>> tuple = new Tuple<>(currMember, null);
				MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEDOWN);
				msgQueue.add(new Message(channel.getAddress(), myMsg));
				return;
			}
		}
		
	}
}
