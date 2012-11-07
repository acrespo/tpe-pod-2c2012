package ar.edu.itba.pod.legajo50758.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import ar.edu.itba.pod.legajo50758.api.Result;
import ar.edu.itba.pod.legajo50758.api.Signal;

public class MyTask implements Callable<Result> {

	private Signal signal;
	private BlockingQueue<SignalInfo> signals;

	public MyTask(Signal signal, BlockingQueue<SignalInfo> signals) {
		this.signal = signal;
		this.signals = signals;
	}
	
	@Override
	public Result call() throws Exception {
		if (signal == null) {
			throw new IllegalArgumentException("Signal cannot be null");
		}

		Result result = new Result(signal);
		
		for (SignalInfo signalInfo : signals) {
			Signal cmp = signalInfo.getSignal();
			Result.Item item = new Result.Item(cmp, signal.findDeviation(cmp));
			result = result.include(item);
		}
		return result;
	}

}
