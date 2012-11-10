package ar.edu.itba.pod.legajo50758.impl;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class MyReceiverAdapter extends ReceiverAdapter {

	private final JChannel channel;
	private final BlockingQueue<Message> msgQueue;
	private View currentView = null;
	private final MyWorker worker;
	private Thread workerThread;
	private final AtomicBoolean degradedMode;

	public MyReceiverAdapter(JChannel channel, BlockingQueue<Message> msgQueue,
			MyWorker worker, Thread workerThread, AtomicBoolean degradedMode) {
		this.channel = channel;
		this.msgQueue = msgQueue;
		this.worker = worker;
		this.workerThread = workerThread;
		this.degradedMode = degradedMode;
	}

	@Override
	public void receive(Message message) {
		
//		Utils.log("Received: %s: %s", new Object[] { message.getObject(), message.getSrc() });
//		if (message.getObject() instanceof MyMessage<?>) {
			msgQueue.add(message);
//		}
	}
	
	@Override
	public void viewAccepted(final View newView) {
		
		System.out.println("TOPOLOGY CHANGE!!!");
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				if (currentView == null) {
					currentView = newView;
					workerThread = new Thread(worker);
					workerThread.start();		
					
					if (newView.size() > 1) {
						
						try {
							worker.phaseEnd(newView.size());
							worker.phaseEnd(newView.size());
							worker.phaseEnd(newView.size());
						} catch (Exception e) {
							e.printStackTrace();
						}
						degradedMode.set(false);
						//TODO resumir trabajos de procesamiento
						
					}
					return;
				}
				System.out.println("Former members: " + currentView.getMembers());
				System.out.println("New members: " + newView.getMembers());
				
				
				for (Address member: newView.getMembers()) {
					if (!currentView.containsMember(member)) {
						// member is NEW NODE
						degradedMode.set(true);
						//TODO para trabajos de procesamiento
						Tuple<Address, List<Address>> tuple = new Tuple<>(member, newView.getMembers());
						MyMessage<Tuple<Address, List<Address>>> myMsg = new MyMessage<>(tuple, Operation.NODEUP);
						msgQueue.add(new Message(channel.getAddress(), myMsg));
						currentView = newView;
						System.out.println("NOW our members are :" + currentView.getMembers());
						return;
					}
				}
				
				for (Address currMember: currentView.getMembers()) {
					if (!newView.containsMember(currMember)) {
						//currMember is DOWN
						degradedMode.set(true);
						//TODO para trabajos de procesamiento
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
		}).start();
	}
	
	public void resetView() {
		currentView = null;
	}
	
}
