package ar.edu.itba.pod.legajo50758.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.Channel;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class MyMessageDispatcher {

	private final AtomicInteger idGenerator = new AtomicInteger(0);
	private final Multimap<Address, MyFutureImpl<?>> addressToFuture;
	private final Map<Integer, MyFutureImpl<?>> futures;
	private final Channel channel;
	
	public MyMessageDispatcher(Channel channel) {
		
		this.channel = channel;
		futures = new ConcurrentHashMap<Integer, MyFutureImpl<?>>();
		addressToFuture = Multimaps.synchronizedMultimap(HashMultimap.<Address, MyFutureImpl<?>>create());
	}
	
	public <T> Future<T> sendMessage(Address address, Serializable obj) {

		final int id = idGenerator.getAndIncrement();
		final MyFutureImpl<T> future = new MyFutureImpl<T>(id);

		addressToFuture.put(address, future);
		futures.put(id, future);

		try {
			channel.send(address, new RequestMessage(id, obj));
		} catch (final Exception e) {
			addressToFuture.remove(address, future);
			futures.remove(id);
			future.nodeDisconnected(e);
		}
		return future;
	}
	
	public void processResponse(Address origin, ResponseMessage response) {

		final MyFutureImpl<?> future = futures.remove(response.getId());
		if (future != null) {
			addressToFuture.remove(origin, future);
			future.setResponse(response.getContent());
		}
	}

	public void respondTo(Address address, int id, Serializable content) throws Exception {
		channel.send(address, new ResponseMessage(id, content));
	}
	
	
	private static class MyFutureImpl<T> implements Future<T> {

		private T response;
		private final CountDownLatch isDone = new CountDownLatch(1);
		private boolean disconnected;
		private Exception e;
		private final int id;
		
		public MyFutureImpl(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}
		
		public void setResponse(Serializable content) {
			
			if (isDone.getCount() == 0 || disconnected) {
				throw new IllegalStateException();
			}

			try {
				response = (T) content;
			} catch (final ClassCastException e) {
				
				response = null;
			}
			isDone.countDown();
			
		}

		public void nodeDisconnected(Exception e) {
			
			if (isDone.getCount() != 0) {
				disconnected = true;
				this.e = e;
			}
			isDone.countDown();
		}
		
		public void nodeDisconnected() {

			if (isDone.getCount() != 0) {
				disconnected = true;
			}
			isDone.countDown();
		}

		@Override
		public boolean cancel(boolean arg0) {
			return false;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			
			isDone.await();
			if (disconnected) {
				throw new ExecutionException("The reciepient disconnected before answering", e);
			}
			return response;
		}

		@Override
		public T get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
			
			if (isDone.await(timeout, timeUnit)) {
				return get();
			}
			throw new TimeoutException();
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return isDone.getCount() == 0;
		}
		
	}
}
