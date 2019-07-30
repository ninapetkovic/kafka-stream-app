/**
 * 
 */
package com.nina.app.kafka_stream.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.nina.app.kafka_stream.request.KafkaRequest;
import com.nina.app.kafka_stream.service.EstimatorService;
import com.nina.app.kafka_stream.service.KafkaService;

/**
 * @author NinaPetkovic
 * @created 28.07.2019.
 * @modified 28.07.2019.
 */

@RestController
@RequestMapping("/api")
public class KafkaController {

	@Autowired
	KafkaService countingService;

	@Autowired
	EstimatorService estimate;


	@RequestMapping(value = "/print_stream", method = RequestMethod.GET)
	public void printStream(@RequestBody KafkaRequest request) {

		System.out.println(request);
		try {
			countingService.printStream(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error ocured!");
		}
	}

	@RequestMapping(value = "/kafka_pipe", method = RequestMethod.GET)
	public void kafkaPipe(@RequestBody KafkaRequest request) {

		System.out.println(request);
		try {
			countingService.kafkaPipe(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error ocured!");
		}
	}


	@RequestMapping(value = "/read_produce", method = RequestMethod.GET)
	public void produceToNewTopic(@RequestBody KafkaRequest request) {

		try {
			countingService.produce(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error occured!");
		}
	}

	@RequestMapping(value = "/estimate_data", method = RequestMethod.GET)
	public void estimateData(@RequestBody KafkaRequest request) {

		try {
			estimate.estimateData(request);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error occured!");
		}
	}

	@RequestMapping(value = "/get_estimator", method = RequestMethod.GET)
	public void getEstimator(@RequestBody KafkaRequest request) {

		try {
			estimate.getEstimator(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error occured!");
		}
	}


}
