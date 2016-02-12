package io.vertx.example;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;

	private final String USER_AGENT = "Mozilla/5.0";
	private final ServerSocket socket;
	// private final List<DataCenterInstance> instances;
	// private final CopyOnWriteArrayList<DataCenterInstance> instances;
	private final List<DataCenterInstance> instances;
	// private final String lock;
	private final TimeWrapper timeWrapper;
	private final ReentrantLock lock;

	private static boolean c = false;
	private int indicator;

	public LoadBalancer(ServerSocket socket,  List<DataCenterInstance> instances, TimeWrapper wrapper, ReentrantLock l) {
		this.socket = socket;
		this.instances = instances;
		this.timeWrapper = wrapper;
		this.lock = l;
	}

	// Complete this function
	public void start() throws IOException {
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		this.indicator = 0;

		Thread healthyChecker = new Thread() {
			public void run() {
				try {
					while (true) {
						if (timeWrapper.period == 0) {
							Thread.sleep(3000);
						} else {
							Thread.sleep(timeWrapper.period * 1000);
							lock.lock();
							Iterator<DataCenterInstance> iterator = instances.iterator();
							while (iterator.hasNext()) {
								DataCenterInstance instance = iterator.next();
								System.out.println("\nHealth check: " + instance.getUrl());
								try {
									URL obj = new URL(instance.getUrl());
									HttpURLConnection con = (HttpURLConnection) obj.openConnection();
									con.setRequestMethod("GET");
									con.setRequestProperty("User-Agent", USER_AGENT);
									con.setConnectTimeout(350);
									con.setReadTimeout(350);

									int responseCode = con.getResponseCode();
									if (responseCode != 200) {
										System.out.println(instance.getUrl() + " died, remove it because not 200!");
//										// instances.remove(i);
										iterator.remove();
										//i--;
									}
								} catch (Exception e) {
									System.out.println(instance.getUrl() + " died, remove it because exception!");
									//instances.remove(i);
									iterator.remove();

									//i--;
								}
							}
							lock.unlock();
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		healthyChecker.start();

		System.out.println("instances shoud be empty now!");
		while (true) {
			if (instances.size() == 0) {
				continue;
			}

			Socket sock = socket.accept();
			lock.lock();
			indicator = (indicator % instances.size());
			System.out.println("route request to " + instances.get(indicator).getUrl());
			Runnable requestHandler = new RequestHandler(sock, instances.get(indicator));
			indicator++;
			lock.unlock();
			executorService.execute(requestHandler);
		}
	}
}