package com.appiancorp.cs.plugin.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class AppianKafkaProducer {
  private static final Logger LOG = Logger.getLogger(AppianKafkaProducer.class);

  private Properties _props;
  private String _exception;
  private boolean _success;

  private Producer<String, String> _producer;

  public AppianKafkaProducer(Properties props) {
    this._props = props;
  }

  public void publish(String topic, String payload) throws Exception {

    if (LOG.isDebugEnabled())
      LOG.debug("Instantiating producer with properties: " + _props);
    _producer = new KafkaProducer<>(_props);

    if (LOG.isDebugEnabled())
      LOG.debug("Sending payload to topic: " + topic);

    _producer.send(new ProducerRecord<>(topic, payload),
      new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception ex) {
          if (ex != null) {
            LOG.error(String.format("Failed to produce record. Got Exception: %s", ex));
            setException(ex.getMessage());
            setSuccess(false);
          } else {
            if (LOG.isDebugEnabled())
              LOG.debug("Sent record successfully. Topic: " + metadata.topic() + "; Partition: " + metadata.partition() + "; Offset: " +
                metadata.offset() + "; Timestamp: " + metadata.timestamp());
            setSuccess(true);
          }

        }
      });

    if (LOG.isDebugEnabled())
      LOG.debug("Sent payload: " + payload);
    _producer.flush();
    _producer.close();
  }

  public void setException(String e) {
    _exception = e;
  }

  public String getException() {
    return _exception;
  }

  public void setSuccess(boolean b) {
    _success = b;
  }

  public boolean getSuccess() {
    return _success;
  }

}
