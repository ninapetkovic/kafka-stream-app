/**
 * 
 */
package com.nina.app.kafkaStream.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.nina.app.kafkaStream.request.KafkaRequest;
import com.nina.app.kafkaStream.service.EstimatorService;
import com.nina.app.kafkaStream.service.KafkaService;

/**
 * @author NinaPetkovic
 * @created 28.07.2019.
 * @modified 28.07.2019.
 */

@RestController
@RequestMapping("/api")
public class KafkaController {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaController.class);
	private static Map<String, String> env = System.getenv();

	@Autowired
	KafkaService countingService;

	@Autowired
	EstimatorService estimate;

	@RequestMapping(value = "/print_stream", method = RequestMethod.GET)
	public void printStream(@RequestBody KafkaRequest request) {

		LOG.info(request.toString());
		try {
			countingService.printStream(request);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Error occured!");
		}
	}

	@RequestMapping(value = "/kafka_pipe", method = RequestMethod.GET)
	public void kafkaPipe(@RequestBody KafkaRequest request) {

		LOG.info(request.toString());
		try {
			countingService.kafkaPipe(request);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Error occurred!");
		}
	}

	@RequestMapping(value = "/read_produce", method = RequestMethod.GET)
	public void produceToNewTopic(@RequestBody KafkaRequest request) {

		LOG.info(request.toString());
		try {
			countingService.produce(request);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Error occurred!");
		}
	}

	@RequestMapping(value = "/estimate_data", method = RequestMethod.GET)
	public void estimateData(@RequestBody KafkaRequest request) {

		LOG.info(request.toString());
		try {
			estimate.estimateData(request);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Error occurred!");
		}
	}

	@RequestMapping(value = "/get_estimator", method = RequestMethod.GET)
	public void getEstimator(@RequestBody KafkaRequest request) {

		LOG.info(request.toString());
		try {
			estimate.getEstimator(request);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.info("Error occurred!");
		}
	}

}
