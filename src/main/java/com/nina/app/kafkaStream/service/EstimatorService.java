package com.nina.app.kafkaStream.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.nina.app.kafkaStream.request.KafkaRequest;

/**
 * @author NinaPetkovic
 * @created 29.07.2019.
 * @modified 29.07.2019.
 */
@Service
public class EstimatorService {

	private static final Logger LOG = LoggerFactory.getLogger(EstimatorService.class);
	@Autowired
	private static Environment env;

	private static String json_key;
	private static HashSet<String> hs;
	private static HyperLogLog hl;
	private static LinearCounting lc;
	private static int hl_par;
	private static int lc_par;
	private static int bytes;
	private static int seconds;

	private static String byteArrayToHex(byte[] a) {
		StringBuilder sb = new StringBuilder(a.length * 2);
		for (byte b : a)
			sb.append(String.format("%02x", b & 0xff));
		return sb.toString();
	}

	private static void initCounting() {
		hl_par = 10;
		lc_par = bytes;
		hs = new HashSet<String>();
		hl = new HyperLogLog(hl_par);
		lc = new LinearCounting(lc_par);

	}

	private static void processCounting(String line) {
		try {
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new StringReader(line));
			JSONObject jsonObject = (JSONObject) obj;
			String val = (String) jsonObject.get(json_key);
			long ts = (Long) jsonObject.get("ts");
			boolean added = false;
			if (hs.add(val)) {
				LOG.info("HASHSET=" + hs.size() + " ");
				added = true;
			}
			if (hl.offer(val)) {
				LOG.info("LOGLOG=" + hl.cardinality() + " ");
				added = true;
			}
			if (lc.offer(val)) {
				LOG.info("LINEAR=" + lc.cardinality() + " ");
				added = true;
			}
			if (added)
				LOG.info("TS=" + ts + " NEW VAL=" + (val + "XXXXXXXXXX").substring(0, 10));
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ParseException ex) {
			ex.printStackTrace();
		} catch (NullPointerException ex) {
			ex.printStackTrace();
		}

	}

	private static void punctuateCounting(long timestamp) {
		LOG.info(new Date(timestamp).toString());
	}

	private static void closeCounting() {
	}

	private static KafkaStreams initKafkaStream(String src_str, String dst_str) {
		java.util.Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("APPLICATION_ID_CONFIG"));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, env.getProperty("ZOOKEEPER_CONNECT_CONFIG"));
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("AUTO_OFFSET_RESET_CONFIG"));
		props.put(StreamsConfig.STATE_DIR_CONFIG, env.getProperty("STATE_DIR_CONFIG"));

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource(env.getProperty("SOURCE_NAME"), src_str);
		builder.addProcessor(env.getProperty("PROCESSOR_NAME"), new DataEstimatorProcessorSupplier(),
				env.getProperty("SOURCE_NAME"));
		builder.addSink(env.getProperty("SINK_NAME"), dst_str, env.getProperty("PROCESSOR_NAME"));

		return new KafkaStreams(builder, props);
	}

	private static class EstimatorSerializer<T> implements Serializer<T> {

		@Override
		public void configure(Map<String, ?> map, boolean b) {
		}

		@Override
		public byte[] serialize(String topic, T t) {
			return estimatorJSON().getBytes();
		}

		@Override
		public void close() {
		}
	}

	private static class DataEstimatorProcessorSupplier implements ProcessorSupplier<String, String> {

		@Override
		public Processor<String, String> get() {
			return new Processor<String, String>() {
				private ProcessorContext context;

				@Override
				@SuppressWarnings("unchecked")
				public void init(ProcessorContext context) {
					this.context = context;
					this.context.schedule(seconds * 1000);
					initCounting();
				}

				@Override
				public void process(String dummy, String line) {
					processCounting(line);
					LOG.info("HASHSET=" + hs.size() + " HYPERLOG=" + hl.cardinality() + " LINEAR=" + lc.cardinality());
				}

				@Override
				public void punctuate(long timestamp) {
					punctuateCounting(timestamp);
					context.forward("ESTIMATOR", estimatorJSON());
					initCounting();
				}

				@Override
				public void close() {
					closeCounting();
				}
			};
		}
	}

	@SuppressWarnings("unchecked")
	private static String estimatorJSON() {
		JSONObject obj = new JSONObject();
		obj.put("ts", new Date().getTime());
		obj.put("range", seconds);
		obj.put("ec", lc_par);
		String hexvalue = byteArrayToHex(lc.getBytes());
		obj.put("est", hexvalue);
		return obj.toJSONString();
	}

	public void estimateData(KafkaRequest request) throws Exception {
		boolean std_in = false;
		KafkaStreams kafka_stream = null;
		BufferedReader br = null;

		String source_stream = request.getSourceTopic();
		String destination_stream = request.getDestinationTopic();
		std_in = source_stream.equals("stdin");
		json_key = request.getJsonKey();
		bytes = Integer.valueOf(request.getBytes()).intValue();
		seconds = Integer.valueOf(request.getBytes()).intValue();
		std_in = source_stream.equals("stdin");
		json_key = request.getJsonKey();

		if (!std_in)
			kafka_stream = initKafkaStream(source_stream, destination_stream);

		if (std_in) {
			try {
				initCounting();
				br = new BufferedReader(new InputStreamReader(System.in));
				String line = "";
				while (true) {
					line = br.readLine();
					if (line == null)
						break;
					processCounting(line);
				}
			} catch (Exception e) {
			}
		} else {
			kafka_stream.start();
		}

		if (std_in) {
			LOG.info(estimatorJSON());
		}

		Thread.sleep(2000000L);
		if (!std_in)
			kafka_stream.close();
	}

	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	private static class GetEstimatorProcessorSupplier implements ProcessorSupplier<String, String> {

		@Override
		public Processor<String, String> get() {
			return new Processor<String, String>() {
				private ProcessorContext context;

				@Override
				@SuppressWarnings("unchecked")
				public void init(ProcessorContext context) {
					this.context = context;
					this.context.schedule(1000);
				}

				@Override
				public void process(String dummy, String line) {

					try {
						LOG.info(line);
						JSONParser parser = new JSONParser();
						Object obj = parser.parse(new StringReader(line));
						JSONObject jsonObject = (JSONObject) obj;
						String est = (String) jsonObject.get("est");
						long ts = (Long) jsonObject.get("ts");
						long range = (Long) jsonObject.get("range");
						long ec = (Long) jsonObject.get("ec");
						byte[] map = hexStringToByteArray(est);
						long c = 0;
						for (byte b : map) {
							c += Integer.bitCount(b & 0xFF);
						}

						JSONObject obj1 = new JSONObject();
						long card = ec * 8 - c;
						obj1.put("est", card);
						obj1.put("ts", ts);
						obj1.put("range", range);
						obj1.put("ec", ec);
						LOG.info(obj1.toJSONString());
					} catch (IOException ex) {
						ex.printStackTrace();
					} catch (ParseException ex) {
						ex.printStackTrace();
					} catch (NullPointerException ex) {
						ex.printStackTrace();
					}
				}

				@Override
				public void punctuate(long timestamp) {
				}

				@Override
				public void close() {
				}
			};
		}
	}

	/**
	 * @param request
	 * @throws Exception
	 */
	public void getEstimator(KafkaRequest request) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getProperty("APPLICATION_ID_CONFIG"));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, env.getProperty("ZOOKEEPER_CONNECT_CONFIG"));
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("AUTO_OFFSET_RESET_CONFIG"));
		props.put(StreamsConfig.STATE_DIR_CONFIG, env.getProperty("STATE_DIR_CONFIG"));

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("Source", request.getSourceTopic());

		builder.addProcessor("Process", new GetEstimatorProcessorSupplier(), "Source");

		KafkaStreams streams = new KafkaStreams(builder, props);

		streams.start();

		// usually the stream application would be running forever,
		// we just let it run for some time and stop since the input data is
		// finite.
		Thread.sleep(150000L);

		LOG.info("Estimation finished!");
		streams.close();

	}
}
