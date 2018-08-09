package rpc;

public abstract class Rpc {
	private boolean running = false;

	public synchronized void join() throws InterruptedException {
		while (running) {
			wait();
		}
	}
	public synchronized void stop() throws InterruptedException {
		while (running) {
			notify();
		}
	}
}
