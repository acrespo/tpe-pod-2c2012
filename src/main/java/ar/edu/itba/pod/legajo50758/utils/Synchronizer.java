package ar.edu.itba.pod.legajo50758.utils;

import java.util.concurrent.Semaphore;


public class Synchronizer {

	private final Semaphore sem = new Semaphore(1);
	private final Object lock = new Object();
	
	public void acquireAndRelease() {
		
		synchronized (lock) {
			try {
				sem.acquire();
				sem.release();			
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int drainPermits() {
		
		synchronized (lock) {	
			return sem.drainPermits();
		}
	}
	
	public void release() {
		sem.release();
	}
}
