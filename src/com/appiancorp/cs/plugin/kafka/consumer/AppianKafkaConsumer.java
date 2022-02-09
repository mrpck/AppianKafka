package com.appiancorp.cs.plugin.kafka.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import com.appiancorp.cs.plugin.kafka.AppianKafkaUtil;
import com.google.gson.JsonObject;

public class AppianKafkaConsumer {

  private static Logger LOG = Logger.getLogger(AppianKafkaConsumer.class);

  private static final Integer TRANSACTION_MANAGER_STATUS_QUEUED = 1;
  private KafkaConsumer<String, String> consumer;

  private boolean exit = false;

  private Context _ctx;
  private String _dsName;
  private Properties _props;
  private String _topic;
  private Integer _jobTypeId;
  private Integer _pollingInterval;
  private Integer _sleepingInterval;
  private String _transactionTableName;
  private String _messageFilter;

  private boolean conAutoCommit;

  private long _id;
  private Connection con;
  private PreparedStatement pstmt;

  public AppianKafkaConsumer(Properties props, String dsName, String transactionTableName, String topic, Integer jobTypeId,
    Integer pollingInterval, Integer sleepingInterval, String messageFilter) {
    _props = props;
    _dsName = dsName;
    _transactionTableName = transactionTableName;
    _topic = topic;
    _pollingInterval = pollingInterval;
    _sleepingInterval = sleepingInterval;
    _jobTypeId = jobTypeId;
    _messageFilter = messageFilter;
  }

  public AppianKafkaConsumer(Properties props, Context context, String dsName, String transactionTableName, String topic, Integer jobTypeId,
    Integer pollingInterval, Integer sleepingInterval, String messageFilter) {
    _ctx = context;
    _props = props;
    _dsName = dsName;
    _transactionTableName = transactionTableName;
    _topic = topic;
    _pollingInterval = pollingInterval;
    _sleepingInterval = sleepingInterval;
    _jobTypeId = jobTypeId;
    _messageFilter = messageFilter;
  }

  public void run() throws Exception {
    if (LOG.isDebugEnabled())
      LOG.debug("Running with id: " + _id);

    try {
      if (LOG.isDebugEnabled())
        LOG.debug(_id + ", getting DB connection");
      con = getConnection(_dsName);
      conAutoCommit = con.getAutoCommit();
      con.setAutoCommit(false);
      boolean filterMessages = StringUtils.isNotBlank(_messageFilter);

      String SQLCommand = "INSERT INTO " + _transactionTableName +
        " (job_type_id, context_json, scheduled_date, status_id) VALUES (?, ?, ?, ?)";
      pstmt = con.prepareStatement(SQLCommand);

      if (LOG.isDebugEnabled())
        LOG.debug(_id + ", creating consumer with following properties: " + _props.toString());
      consumer = new KafkaConsumer<>(_props);

      if (LOG.isDebugEnabled())
        LOG.debug(_id + ", querying Kafka for available topics");
      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      if (LOG.isDebugEnabled()) {
        LOG.debug(_id + ", topics available:");
        topics.forEach((k, v) -> LOG.debug(_id + "  - Name: " + k + ", Partition Info: " + v));
      }

      if (LOG.isDebugEnabled())
        LOG.debug(_id + ", subscribing to topic: " + _topic);
      consumer.subscribe(Arrays.asList(_topic));

      while (!exit) {
        if (LOG.isDebugEnabled())
          LOG.debug(_id + ", retrieving records from topic: " + _topic + " with polling interval of " + _pollingInterval +
            ", sleeping interval of " + _sleepingInterval);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(_pollingInterval));
        if (LOG.isDebugEnabled())
          LOG.debug(_id + ", received Number of Records: " + records.count());

        if (records.count() > 0) {
          int batchCounter = 0;
          for (ConsumerRecord<String, String> record : records) {
            if (LOG.isTraceEnabled())
              LOG.trace(_id + ", record: " + record.toString());

            String value = record.value();

            // If the message should be ignored, skip processings
            if (filterMessages && AppianKafkaUtil.ignoreMesssage(value, _messageFilter))
              continue;

            JsonObject jo = new JsonObject();
            jo.addProperty("topic", record.topic());
            jo.addProperty("partition", record.partition());
            jo.addProperty("offset", record.offset());
            jo.addProperty("key", record.key());
            jo.addProperty("value", value);

            pstmt.setInt(1, _jobTypeId);
            pstmt.setString(2, jo.toString());
            pstmt.setNull(3, java.sql.Types.NULL);
            pstmt.setInt(4, TRANSACTION_MANAGER_STATUS_QUEUED);

            if (LOG.isTraceEnabled())
              LOG.trace("Adding SQL to batch: " + pstmt.toString());
            pstmt.addBatch();

            batchCounter++;
            if (batchCounter % 1000 == 0) {
              if (LOG.isDebugEnabled())
                LOG.debug("Executing batch inside the loop");
              pstmt.executeBatch();
            }

          }

          if (LOG.isDebugEnabled())
            LOG.debug("Finished looping through records, executing any remaining batches and committing.");
          pstmt.executeBatch();
          con.commit();

          // Q: should we move this after the process instance has been started? Can we really consider that the records have been processed
          // before that?
          // A: The records are saved the database which means that the Consumer has processed them. We don't want to receive duplicate
          // records in case a Process Model fails because
          // that is outside of the responsibility of the consumer. Also, there may not be a process supplied to the consumer to start in a
          // different use case.

          if (LOG.isDebugEnabled())
            LOG.debug(_id + ", manually committing all in this iteration");
          consumer.commitSync();
        }

        if (LOG.isDebugEnabled())
          LOG.debug(_id + " sleeping for " + _sleepingInterval.intValue());
        Thread.sleep(_sleepingInterval.intValue());
        if (LOG.isDebugEnabled())
          LOG.debug(_id + " waking up");
      }

    } catch (SQLException se) {
      LOG.error(se.getMessage(), se);
      throw se;
    } catch (NamingException ne) {
      LOG.error(ne.getMessage(), ne);
      throw ne;
    } finally {
      shutdown();
      if (LOG.isDebugEnabled())
        LOG.debug(_id + "finished");
    }
  }

  private Connection getConnection(String dataSourceName) throws NamingException, SQLException {
    DataSource obj = (DataSource) _ctx.lookup(dataSourceName);
    return obj.getConnection();
  }

  public void setId(long id) {
    _id = id;
  }

  public long getId() {
    return _id;
  }

  public void stop() {
    exit = true;
  }

  public void shutdown() {
    try {
      if (pstmt != null)
        pstmt.close();
    } catch (Exception e) {
    }
    try {
      if (con != null)
        con.setAutoCommit(conAutoCommit);
      con.close();
    } catch (Exception e) {
    }
    try {
      if (consumer != null)
        consumer.close();
    } catch (Exception e) {
    }
  }

}
