package io.vertx.example;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.*;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;

	private final String USER_AGENT = "Mozilla/5.0";
	private final ServerSocket socket;
	// private final List<DataCenterInstance> instances;
	private final CopyOnWriteArrayList<DataCenterInstance> instances;
	// private final String lock;
	private final TimeWrapper timeWrapper;

	private static boolean c = false;
	private int indicator;

	public LoadBalancer(ServerSocket socket, CopyOnWriteArrayList<DataCenterInstance> instances, TimeWrapper wrapper) {
		this.socket = socket;
		this.instances = instances;
		this.timeWrapper = wrapper;
		// lock = null;
	}

//	public LoadBalancer(ServerSocket socket, CopyOnWriteArrayList<DataCenterInstance> instances, String lock, TimeWrapper wrapper) {
//		this.socket = socket;
//		this.instances = instances;
//		//this.lock = lock;
//		this.timeWrapper = wrapper;
//	}

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
							for (int i = 0 ; i < instances.size(); i++) {
								DataCenterInstance instance = instances.get(i);
								System.out.println("\nHealth check: " + instance.getUrl());
								try {
									URL obj = new URL(instance.getUrl());
									HttpURLConnection con = (HttpURLConnection) obj.openConnection();
									con.setRequestMethod("GET");
									con.setRequestProperty("User-Agent", USER_AGENT);
									con.setConnectTimeout(2000);
									//con.setReadTimeout(5000);

									int responseCode = con.getResponseCode();
									if (responseCode != 200) {
										System.out.println(instance.getUrl() + " died, remove it because not 200!");
										instances.remove(i);
										i--;
									}
								} catch (Exception e) {
									System.out.println(instance.getUrl() + " died, remove it because exception!");
									instances.remove(i);
									i--;
								}
							}							}
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
			System.out.println("instances has elements now!");

			Socket sock = socket.accept();
			indicator = (indicator % instances.size());
			System.out.println("route request to " + instances.get(indicator).getUrl());
			Runnable requestHandler = new RequestHandler(sock, instances.get(indicator));
			indicator++;
			executorService.execute(requestHandler);
		}
	}
}
