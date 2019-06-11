package com.rudysysu.es.restclient;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.alibaba.fastjson.JSONObject;

@SpringBootApplication
public class BulkIndex {
	private static final Logger LOG = LoggerFactory.getLogger(BulkIndex.class);

	private static final int TOTAL_RECORD_NUM = 10000 * 2000;
	private static final int THREAD_NUM = 1;
	private static final int BATCH_SIZE = TOTAL_RECORD_NUM / THREAD_NUM;
	private static final int BULK_SIZE = 1000;

	private static long startTime = 0;
	static {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		try {
			Date date = sdf.parse("2019-03-28T00:00:00.000Z");
			startTime = date.getTime();
		} catch (ParseException e) {
			LOG.error(e.toString(), e);
		}
	}
	private static final int MAX_OFFSET = 1000 * 60 * 60 * 24;

	private static final AtomicInteger progress = new AtomicInteger(0);

	private static final CountDownLatch latch = new CountDownLatch(THREAD_NUM);

	private static volatile boolean stopped = false;

	public static void main(String[] args) throws InterruptedException {
		startStatisticThread();

		for (int i = 0; i < THREAD_NUM; i++) {
			final int start = i * BATCH_SIZE + 1;
			final int end = (i + 1) * BATCH_SIZE;
			new Thread("WORKER-" + i) {
				public void run() {
					try {
						indexDocumentInBulk(start, end);
					} catch (InterruptedException e) {
						LOG.error(e.toString(), e);
					}
				}
			}.start();
		}

		latch.await();
		stopped = true;
	}

	private static void indexDocumentInBulk(int start, int end) throws InterruptedException {
		LOG.info("start: {}, end: {}", start, end);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		Random random = new Random();

		try (RestClient client = RestClient.builder(new HttpHost("192.168.37.131", 9200, "http"))
				.setRequestConfigCallback(new RequestConfigCallback() {
					public Builder customizeRequestConfig(Builder requestConfigBuilder) {
						requestConfigBuilder.setConnectionRequestTimeout(5000);
						requestConfigBuilder.setConnectTimeout(5000);
						requestConfigBuilder.setSocketTimeout(5000);
						return requestConfigBuilder;
					}
				}).build();) {

			StringBuilder bulkRequestBody = new StringBuilder();
			int count = 0;
			for (int i = start; i <= end; i++) {
				String timestamp = sdf.format(new Date(startTime + random.nextInt(MAX_OFFSET)));

				String actionMetaData = buildHeader(i);
				String bulkItem = buildBody(i, timestamp, sdf, random);

				bulkRequestBody.append(actionMetaData);
				bulkRequestBody.append("\n");
				bulkRequestBody.append(bulkItem);
				bulkRequestBody.append("\n");

				count++;

				if (count == BULK_SIZE) {
					// LOG.warn(bulkRequestBody.toString());
					HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
					Response response = client.performRequest("POST", "_bulk", Collections.<String, String>emptyMap(),
							entity);
					LOG.info("response: {}", response);

					bulkRequestBody = new StringBuilder();
					progress.addAndGet(count);
					count = 0;
				}
			}
			if (count > 0) {
				// LOG.warn(bulkRequestBody.toString());
				HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
				Response response = client.performRequest("POST", "_bulk", Collections.<String, String>emptyMap(),
						entity);
				LOG.info("response: {}", response);

				bulkRequestBody = new StringBuilder();
				progress.addAndGet(count);
				count = 0;
			}
		} catch (IOException e) {
			LOG.error(e.toString(), e);
		} finally {
			latch.countDown();
		}
	}

	private static String buildHeader(int id) {
		JSONObject metadata = new JSONObject();
		metadata.put("_index", "gis-2019-02-27");
		metadata.put("_type", "gisdata");
		metadata.put("_id", id);

		JSONObject header = new JSONObject();
		header.put("create", metadata);

		return header.toJSONString();
	}

	private static String buildBody(int id, String timestamp, SimpleDateFormat sdf, Random random) {
		JSONObject position = new JSONObject();
		position.put("lat", random.nextInt(90));
		position.put("lon", random.nextInt(180));

		JSONObject body = new JSONObject();
		body.put("driverLicenseNum", "driverLicenseNum_" + id);
		body.put("driverName", "driverName_" + id);
		body.put("enterpriseId", "enterpriseId_" + id);
		body.put("enterpriseName", "enterpriseName_" + id);
		body.put("enterpriseType", "enterpriseType_" + id);
		body.put("fleetId", "fleetId_" + id);
		body.put("oemId", "oemId_" + id);
		body.put("position", position);
		body.put("timestamp", timestamp);
		body.put("vehicleType", "vehicleType_" + id);
		body.put("vin", "vin_" + id);
		body.put("vrn", "vrn_" + id);

		return body.toJSONString();
	}

	private static void startStatisticThread() {
		new Thread() {
			public void run() {
				try {
					long start = System.currentTimeMillis();
					while (true) {
						int current = progress.get();
						int ratio = current * 100 / TOTAL_RECORD_NUM;
						int duration = (int) ((System.currentTimeMillis() - start) / 1000);
						int rate = 0;
						if (duration > 0) {
							rate = current / duration;
						} else if (duration < 0) {
							LOG.error("duration < 0");
						}
						LOG.info("progress: {} - {}%, rate/s: {}", current, ratio, rate);
						if (!stopped) {
							Thread.sleep(5000);
						} else {
							break;
						}
					}
				} catch (InterruptedException e) {
					LOG.error(e.toString(), e);
				}
			}
		}.start();
	}
}
