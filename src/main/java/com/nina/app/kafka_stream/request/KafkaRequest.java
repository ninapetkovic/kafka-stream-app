/**
 * 
 */
package com.nina.app.kafka_stream.request;

/**
 * @author NinaPetkovic
 * @created 28.07.2019.
 * @modified 28.07.2019.
 * @modifiedBy NinaPetkovic
 * @jira
 */
public class KafkaRequest {

	String sourceTopic;
	String destinationTopic;
	String jsonKey;
	String bytes;
	String seconds;

	/**
	 * @return the sourceTopic
	 */
	public String getSourceTopic() {
		return sourceTopic;
	}

	/**
	 * @param sourceTopic the sourceTopic to set
	 */
	public void setSourceTopic(String sourceTopic) {
		this.sourceTopic = sourceTopic;
	}

	/**
	 * @return the destinationTopic
	 */
	public String getDestinationTopic() {
		return destinationTopic;
	}

	/**
	 * @param destinationTopic the destinationTopic to set
	 */
	public void setDestinationTopic(String destinationTopic) {
		this.destinationTopic = destinationTopic;
	}

	/**
	 * @return the jsonKey
	 */
	public String getJsonKey() {
		return jsonKey;
	}

	/**
	 * @param jsonKey the jsonKey to set
	 */
	public void setJsonKey(String jsonKey) {
		this.jsonKey = jsonKey;
	}

	/**
	 * @return the bytes
	 */
	public String getBytes() {
		return bytes;
	}

	/**
	 * @param bytes the bytes to set
	 */
	public void setBytes(String bytes) {
		this.bytes = bytes;
	}

	/**
	 * @return the seconds
	 */
	public String getSeconds() {
		return seconds;
	}

	/**
	 * @param seconds the seconds to set
	 */
	public void setSeconds(String seconds) {
		this.seconds = seconds;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KafkaRequest [sourceTopic=" + sourceTopic + ", destinationTopic=" + destinationTopic + ", jsonKey="
				+ jsonKey + ", bytes=" + bytes + ", seconds=" + seconds + "]";
	}

}
