package com.nina.app.kafka_stream.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashSet;
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
import org.springframework.stereotype.Service;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.nina.app.kafka_stream.request.KafkaRequest;

/**
 * @author NinaPetkovic
 * @created 28.07.2019.
 * @modified 28.07.2019.
 */
@Service
public class KafkaService {

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
				System.out.println(line);
			} else {
				JSONParser parser = new JSONParser();
				Object obj = parser.parse(new StringReader(line));
				JSONObject jsonObject = (JSONObject) obj;
				String val = (String) jsonObject.get(json_key);
				boolean added = false;
				if (hs.add(val)) {
					System.out.print("HASHSET=" + hs.size() + " ");
					added = true;
				}
				if (hl.offer(val)) {
					System.out.print("LOGLOG=" + hl.cardinality() + " ");
					added = true;
				}
				if (lc.offer(val)) {
					System.out.print("LINEAR=" + lc.cardinality() + " ");
					added = true;
				}
				if (added)
					System.out.println(" NEW VAL=" + val);
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
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.STATE_DIR_CONFIG, "{goodDir}");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return props;
	}

	private static KafkaStreams initKafkaStreamConsumer(String src_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("Source", src_str);

		builder.addProcessor("Process", new KafkaPipeProcessorSupplier(), "Source");

		return new KafkaStreams(builder, props);
	}

	private static KafkaStreams initKafkaStreamPrint(String src_str, String dest_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("Source", src_str);

		builder.addProcessor("Process", new KafkaPipeProcessorSupplier(), "Source");

		return new KafkaStreams(builder, props);
	}

	private static KafkaStreams initKafkaStreamProducer(String src_str, String dest_str) {

		Properties props = initKafkaStream();

		TopologyBuilder builder = new TopologyBuilder();

		builder.addSource("Source", src_str);

		builder.addProcessor("Process", new KafkaPipeProcessorSupplier(), "Source");

		builder.addSink("Sink", dest_str, "Source");

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
						System.out.println(
							"HASHSET=" + hs.size() + " HYPERLOG=" + hl.cardinality() + " LINEAR=" + lc.cardinality());

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

		System.out.println("-----------------------");
		System.out.println("TOPIC " + source_stream);
		System.out.println("JSON KEY " + json_key);
		System.out.println("USING: HASHSET LOGLOG LINEAR");
		System.out.println("-----------------------");
		System.out.println("BEGINNING OF TOPIC DATA");
		System.out.println("-----------------------");

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

		System.out.println("-----------------------");
		System.out.println("END OF TOPIC DATA");
		System.out.println("-----------------------");

		if (!std_in)
			kafka_stream.close();

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

		System.out.println("-----------------------");
		System.out.println("TOPIC " + source_stream);
		System.out.println("JSON KEY " + json_key);
		System.out.println("USING: HASHSET LOGLOG LINEAR");
		System.out.println("-----------------------");
		System.out.println("BEGINNING OF TOPIC DATA");
		System.out.println("-----------------------");

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
		System.out.println("-----------------------");
		System.out.println("END OF TOPIC DATA");
		System.out.println("-----------------------");

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

		System.out.println("-----------------------");
		System.out.println("TOPIC " + source_stream);
		System.out.println("JSON KEY " + json_key);
		System.out.println("USING: HASHSET LOGLOG LINEAR");
		System.out.println("-----------------------");
		System.out.println("BEGINNING OF TOPIC DATA");
		System.out.println("-----------------------");

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
		System.out.println("-----------------------");
		System.out.println("END OF TOPIC DATA");
		System.out.println("-----------------------");
	}

}
