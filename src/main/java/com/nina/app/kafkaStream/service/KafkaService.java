package com.nina.app.kafkaStream.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
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
import org.springframework.stereotype.Service;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.nina.app.kafkaStream.request.KafkaRequest;

/**
 * @author NinaPetkovic
 * @created 28.07.2019.
 * @modified 28.07.2019.
 */
@Service
public class KafkaService {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
	private static Map<String, String> env = System.getenv();

	private static String json_key;
	private static HashSet<String> hs;
	private static HyperLogLog hl;
	private static LinearCounting lc;
	private static boolean print;

	private static void initCounting() {

		hs = new HashSet<String>();
		hl = new HyperLogLog(10);
		lc = new LinearCounting(150000);
	}

	private static void processCounting(String line) {
		try {
			if (print) {
				LOG.info(line);
			} else {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(new StringReader(line));
				JSONObject jsonObject = (JSONObject) obj;
				String val = (String) jsonObject.get(json_key);
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
					LOG.info(" NEW VAL=" + val);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ParseException ex) {
			ex.printStackTrace();
		} catch (NullPointerException ex) {
			ex.printStackTrace();
		}

	}

	private static void punctuateCounting(long timestamp) {
	}

	private static void closeCounting() {
	}

	private static Properties initKafkaStream() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.get("APPLICATION_ID_CONFIG"));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env.get("BOOTSTRAP_SERVERS_CONFIG"));
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, env.get("ZOOKEEPER_CONNECT_CONFIG"));
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.STATE_DIR_CONFIG, env.get("STATE_DIR_CONFIG"));
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.get("AUTO_OFFSET_RESET_CONFIG"));

		return props;
	}

	private static KafkaStreams initKafkaStreamConsumer(String src_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource(env.get("SOURCE_NAME"), src_str);

		builder.addProcessor(env.get("PROCESSOR_NAME"), new KafkaPipeProcessorSupplier(), env.get("SOURCE_NAME"));

		return new KafkaStreams(builder, props);
	}

	private static KafkaStreams initKafkaStreamPrint(String src_str, String dest_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource(env.get("SOURCE_NAME"), src_str);

		builder.addProcessor(env.get("PROCESSOR_NAME"), new KafkaPipeProcessorSupplier(), env.get("SOURCE_NAME"));

		return new KafkaStreams(builder, props);
	}

	private static KafkaStreams initKafkaStreamProducer(String src_str, String dest_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource(env.get("SOURCE_NAME"), src_str);

		builder.addProcessor(env.get("PROCESSOR_NAME"), new KafkaPipeProcessorSupplier(), env.get("SOURCE_NAME"));

		builder.addSink(env.get("SINK_NAME"), dest_str, env.get("SOURCE_NAME"));

		return new KafkaStreams(builder, props);
	}

	private static class KafkaPipeProcessorSupplier implements ProcessorSupplier<String, String> {

		@Override
		public Processor<String, String> get() {
			return new Processor<String, String>() {
				private ProcessorContext context;

				@Override
				@SuppressWarnings("unchecked")
				public void init(ProcessorContext context) {
					this.context = context;
					this.context.schedule(1000);
					initCounting();
				}

				@Override
				public void process(String dummy, String line) {
					processCounting(line);
					if (!print)
						LOG.info("HASHSET=" + hs.size() + " HYPERLOG=" + hl.cardinality() + " LINEAR="
								+ lc.cardinality());
				}

				@Override
				public void punctuate(long timestamp) {
					punctuateCounting(timestamp);
				}

				@Override
				public void close() {
					closeCounting();
				}
			};
		}
	}

	/**
	 * @param request
	 * @throws Exception
	 */
	public void kafkaPipe(KafkaRequest request) throws Exception {
		boolean std_in = false;
		KafkaStreams kafka_stream = null;
		BufferedReader br = null;

		String source_stream = request.getSourceTopic();
		std_in = source_stream.equals("stdin");
		json_key = request.getJsonKey();

		if (!std_in)
			kafka_stream = initKafkaStreamConsumer(source_stream);

		LOG.info("-----------------------");
		LOG.info("TOPIC " + source_stream);
		LOG.info("JSON KEY " + json_key);
		LOG.info("USING: HASHSET LOGLOG LINEAR");
		LOG.info("-----------------------");
		LOG.info("BEGINNING OF TOPIC DATA");
		LOG.info("-----------------------");

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
		} else
			kafka_stream.start();

		// avoid running forever
		Thread.sleep(150000L);

		if (!std_in)
			kafka_stream.close();

		LOG.info("-----------------------");
		LOG.info("END OF TOPIC DATA");
		LOG.info("-----------------------");
	}

	/**
	 * @param request
	 * @throws Exception
	 */
	public void printStream(KafkaRequest request) throws Exception {
		boolean std_in = false;
		print = true;
		KafkaStreams kafka_stream = null;
		BufferedReader br = null;

		String source_stream = request.getSourceTopic();
		String destination_stream = request.getDestinationTopic();
		std_in = source_stream.equals("stdin");
		json_key = request.getJsonKey();

		if (!std_in)
			kafka_stream = initKafkaStreamPrint(source_stream, destination_stream);

		LOG.info("-----------------------");
		LOG.info("TOPIC " + source_stream);
		LOG.info("JSON KEY " + json_key);
		LOG.info("USING: HASHSET LOGLOG LINEAR");
		LOG.info("-----------------------");
		LOG.info("BEGINNING OF TOPIC DATA");
		LOG.info("-----------------------");

		if (std_in) {
			try {
				initCounting();
				br = new BufferedReader(new InputStreamReader(System.in));
				String line = "";
				while (true) {
					line = br.readLine();
					if (line == null)
						break;
					System.out.print(line);
				}
			} catch (Exception e) {
			}
		} else
			kafka_stream.start();

		// avoid running forever
		Thread.sleep(150000L);

		if (!std_in)
			kafka_stream.close();
		print = false;

		LOG.info("-----------------------");
		LOG.info("END OF TOPIC DATA");
		LOG.info("-----------------------");

	}

	/**
	 * @param request
	 * @throws Exception
	 */
	public void produce(KafkaRequest request) throws Exception {
		boolean std_in = false;
		KafkaStreams kafka_stream = null;
		BufferedReader br = null;

		String source_stream = request.getSourceTopic();
		String destination_stream = request.getDestinationTopic();
		std_in = source_stream.equals("stdin");
		json_key = request.getJsonKey();

		if (!std_in)
			kafka_stream = initKafkaStreamProducer(source_stream, destination_stream);

		LOG.info("-----------------------");
		LOG.info("TOPIC " + source_stream);
		LOG.info("JSON KEY " + json_key);
		LOG.info("USING: HASHSET LOGLOG LINEAR");
		LOG.info("-----------------------");
		LOG.info("BEGINNING OF TOPIC DATA");
		LOG.info("-----------------------");

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
		} else
			kafka_stream.start();

		// avoid running forever
		Thread.sleep(150000L);

		if (!std_in)
			kafka_stream.close();

		LOG.info("-----------------------");
		LOG.info("END OF TOPIC DATA");
		LOG.info("-----------------------");
	}

}
