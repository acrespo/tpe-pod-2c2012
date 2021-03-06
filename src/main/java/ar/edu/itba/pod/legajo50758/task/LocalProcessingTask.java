package ar.edu.itba.pod.legajo50758.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import ar.edu.itba.pod.api.Result;
import ar.edu.itba.pod.api.Signal;
import ar.edu.itba.pod.legajo50758.utils.SignalInfo;

public class LocalProcessingTask implements Callable<Result> {

	private Signal signal;
	private BlockingQueue<SignalInfo> signals;

	public LocalProcessingTask(Signal signal, BlockingQueue<SignalInfo> signals) {
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
