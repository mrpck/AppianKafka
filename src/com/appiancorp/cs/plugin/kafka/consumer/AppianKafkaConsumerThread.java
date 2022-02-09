package com.appiancorp.cs.plugin.kafka.consumer;

import java.util.Properties;
import org.apache.log4j.Logger;

import javax.naming.Context;

public class AppianKafkaConsumerThread extends Thread {

  private static final Logger LOG = Logger.getLogger(AppianKafkaConsumerThread.class);

  private boolean _running = true;
  private String _exception = null;

  private AppianKafkaConsumer _consumer;

  public AppianKafkaConsumerThread(Context ctx, Properties props, String dsName, String transactionTableName, String topic,
    Integer jobTypeId,
    Integer pollingInterval, Integer sleepingInterval, String messageFilter) {
    _consumer = new AppianKafkaConsumer(props, ctx, dsName, transactionTableName, topic, jobTypeId, pollingInterval, sleepingInterval,
      messageFilter);
  }

  public void run() {
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("Starting consumer with id: " + this.getId());

      _consumer.setId(this.getId());
      _consumer.run();

      if (LOG.isDebugEnabled())
        LOG.debug("Finished running consumer with id: " + this.getId());

      _running = false;
    } catch (InterruptedException ie) {
      LOG.error("Received an exception from consumer: " + _consumer.getId() + " - " + ie.getMessage(), ie);
      _consumer.stop();
      _consumer.shutdown();
      _running = false;
      _exception = ie.getMessage();
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.error("Received an exception from consumer: " + _consumer.getId() + " - " + e.getMessage(), e);
      _running = false;
      _exception = e.getMessage();
    }

  }

  public boolean isRunning() {
    return _running;
  }

  public String getException() {
    return _exception;
  }

  public void stopConsumer() {
    _consumer.stop();
  }

}
