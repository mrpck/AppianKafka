package com.appiancorp.cs.plugin.kafka.smartservice;

import java.util.ArrayList;
import java.util.Properties;
import javax.naming.Context;

import com.appiancorp.cs.plugin.kafka.consumer.AppianKafkaConsumerThread;
import com.appiancorp.cs.plugin.kafka.AppianKafkaUtil;
import com.appiancorp.suiteapi.content.ContentService;
import com.appiancorp.suiteapi.knowledge.DocumentDataType;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.MessageContainer;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;

import org.apache.commons.lang.StringEscapeUtils;

@PaletteInfo(paletteCategory = "Integration Services", palette = "Connectivity Services")
@Order({
  // inputs
  "Servers", "DataSourceName", "TransactionTableName",
  "SecurityProtocol", "SecureCredentialsStoreKey", "SASLMechanism", "Truststore", "Keystore",

  "GroupId", "NumConsumers", "Topic", "JobTypeId",
  "RuntimeInMinutes", "PollingIntervalInMs", "SleepingIntervalInMs", "SessionTimeout",
  "DeserializerClassName", "MessageFilter",

  // outputs
  "Success", "ErrorMessage"
})

public class ConsumeKafkaSmartService extends AppianSmartService {

  private static final Logger LOG = Logger.getLogger(ConsumeKafkaSmartService.class);

  private static final int SESSION_TIMEOUT_MS = 30000;
  private static final int MIN_SESSION_TIMEOUT_MS = 10000;
  private static final int MIN_CONSUMERS = 1;
  private static final int MAX_CONSUMERS = 5;
  private static final int MIN_RUNTIME_MIN = 1;
  private static final int MAX_RUNTIME_MIN = 58;
  private static final int MIN_POLL_MS = 1000;
  private static final int MAX_POLL_MS = 300000; /* 5 minute */
  private static final int MIN_SLEEP_MS = 1000;
  private static final int MAX_SLEEP_MS = 1800000; /* 30 minutes */

  private static final String DESERIALIZER_CLASS_NAME = "org.apache.kafka.common.serialization.StringDeserializer";

  private static final Long THREAD_SLEEPING_DURATION_MS = 10000L;

  private final SecureCredentialsStore _scs;
  private final Context _ctx;
  private final ContentService _cs;
  private Properties _props = new Properties();

  // Inputs
  private String _storeKey;
  private String _servers;
  private String _groupId;
  private String _topic;
  private Integer _numConsumers;
  private Integer _jobTypeId;
  private Integer _pollingIntervalMs;
  private Integer _sleepingIntervalMs;
  private Integer _runtimeInMinutes;
  private Integer _sessionTimeoutMs;
  private String _securityProtocol;
  private String _saslMechanism;
  private String _deserializerClassName;
  private String _dsName;
  private String _transactionTableName;
  private Long _trustStoreDoc;
  private Long _keyStoreDoc;
  private String _messageFilter;

  // Outputs
  private Boolean _success;
  private String _errorMessage = "";

  public ConsumeKafkaSmartService(Context ctx, SecureCredentialsStore scs, ContentService cs) {
    super();
    _scs = scs;
    _ctx = ctx;
    _cs = cs;
  }

  @Override
  public void validate(MessageContainer messageContainer) {
    super.validate(messageContainer);

    /* Validate Security Protocol */
    try {
      AppianKafkaUtil.SecurityProtocol sp = AppianKafkaUtil.SecurityProtocol.valueOf(_securityProtocol);

      switch (sp) {
      /* case SSL: */ // TODO verify is needed for SSL
      case SASL_SSL:
      case SASL_PLAINTEXT:
        if (_storeKey == null) {
          messageContainer.addError("SecureCredentialsStoreKey", "storeKey.missing");
        }
        break;
      }

      switch (sp) {
      case SSL:
        /* case SASL_SSL: */ // TODO verify if needed for SASL_SSL
        if (_trustStoreDoc == null && _keyStoreDoc == null) {
          messageContainer.addError("Truststore", "storeDocs.missing");
        }
        break;
      }
    } catch (IllegalArgumentException e) {
      messageContainer.addError("SecurityProtocol", "securityProtocol.invalid");
    }

    /* Validate SASL Mechanism */
    try {
      AppianKafkaUtil.SaslMechanism sp = AppianKafkaUtil.SaslMechanism.valueOf(_saslMechanism);
    } catch (IllegalArgumentException e) {
      messageContainer.addError("SASLMechanism", "saslMechanism.invalid");
    }

    /* Validate Num Consumers */
    if (_numConsumers > MAX_CONSUMERS) {
      messageContainer.addError("NumConsumers", "numConsumers.high");
    }

    if (_numConsumers < MIN_CONSUMERS) {
      messageContainer.addError("NumConsumers", "numConsumers.low");
    }

    /* Validate RuntimeInMinutes */
    if (_runtimeInMinutes > MAX_RUNTIME_MIN) {
      messageContainer.addError("RuntimeInMinutes", "maxRuntime.high");
    }

    if (_runtimeInMinutes < MIN_RUNTIME_MIN) {
      messageContainer.addError("RuntimeInMinutes", "maxRuntime.low");
    }

    /* Validate PollingIntervalInMs */
    if (_pollingIntervalMs > MAX_POLL_MS) {
      messageContainer.addError("PollingIntervalInMs", "pollingInterval.high");
    }

    if (_pollingIntervalMs < MIN_POLL_MS) {
      messageContainer.addError("PollingIntervalInMs", "pollingInterval.low");
    }

    /* Validate SleepingIntervalInMs */
    if (_sleepingIntervalMs > MAX_SLEEP_MS) {
      messageContainer.addError("SleepingIntervalInMs", "sleepingInterval.high");
    }

    if (_sleepingIntervalMs < MIN_SLEEP_MS) {
      messageContainer.addError("SleepingIntervalInMs", "sleepingInterval.low");
    }

    if (StringUtils.isNotBlank(_messageFilter) && !AppianKafkaUtil.validateMessageFilter(_messageFilter)) {
      messageContainer.addError("MessageFilter", "messageFilter.invalid");
    }

    if (_sessionTimeoutMs < MIN_SESSION_TIMEOUT_MS) {
      messageContainer.addError("SessionTimeout", "sessionTimeout.low");
    }

  }

  @Override
  public void run() {
    _success = true;

    try {

      AppianKafkaUtil.setProperties(_scs, _cs, _props, _servers, _securityProtocol, _saslMechanism, _storeKey, _groupId,
        _deserializerClassName, _deserializerClassName, _sessionTimeoutMs, _trustStoreDoc, _keyStoreDoc, true);

      runConsumersForGroup();

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      _errorMessage = _errorMessage + StringEscapeUtils.unescapeHtml(e.getMessage());
      _success = false;
      if (LOG.isDebugEnabled())
        LOG.debug("Returning with success: " + _success + " and error message: " + _errorMessage);
    }
  }

  private void runConsumersForGroup() throws Exception {
    long runStartTime = System.currentTimeMillis();
    ArrayList<AppianKafkaConsumerThread> consumerThreads = new ArrayList<AppianKafkaConsumerThread>();

    for (int counter = 0; counter < _numConsumers; counter++) {
      if (LOG.isDebugEnabled())
        LOG.debug("Starting consumer thread #" + counter);

      AppianKafkaConsumerThread consumerThread = new AppianKafkaConsumerThread(_ctx, _props,
        _dsName,
        _transactionTableName,
        _topic,
        _jobTypeId,
        _pollingIntervalMs,
        _sleepingIntervalMs, _messageFilter);
      consumerThreads.add(consumerThread);
      consumerThread.start();
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Finished starting all consumer threads");

    _success = true;

    long runTimeInMS = _runtimeInMinutes * 60 * 1000;
    long plannedRunStopTime = runStartTime + runTimeInMS;

    while (true) {
      boolean allStopped = true;
      for (AppianKafkaConsumerThread consumer : consumerThreads) {
        if (!consumer.isRunning()) {
          if (LOG.isDebugEnabled())
            LOG.debug("Consumer: " + consumer.getId() + " finished");

          String errorMessage = consumer.getException();
          if (!(errorMessage == null || "".equals(errorMessage) || errorMessage.length() == 0)) {
            if (LOG.isDebugEnabled())
              LOG.debug("Consumer: " + consumer.getId() + " has error message: " + errorMessage);

            _success = false;
            _errorMessage += consumer.getId() + ": " + errorMessage + ";\n";
          }
        } else {
          if (LOG.isDebugEnabled())
            LOG.debug("Consumer: " + consumer.getId() + " is running ok");

          allStopped = false;
        }
      }

      if (allStopped) {
        if (LOG.isDebugEnabled())
          LOG.debug("Found that all consumers have stopped. Exiting monitoring loop");
        break;
      }

      if (plannedRunStopTime > System.currentTimeMillis() + THREAD_SLEEPING_DURATION_MS) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Consumers can keep running for " + (plannedRunStopTime - System.currentTimeMillis()) + " ms");
          LOG.debug("Sleeping for " + THREAD_SLEEPING_DURATION_MS + " ms");

        }
        Thread.sleep(THREAD_SLEEPING_DURATION_MS);
      } else {
        // Stop all threads and wrap up
        if (LOG.isDebugEnabled()) {
          LOG.debug("Runtime expired. Stopping all consumers");
        }
        for (AppianKafkaConsumerThread consumer : consumerThreads) {
          consumer.stopConsumer();
        }
        // Sleeping while the threads stop
        Thread.sleep(THREAD_SLEEPING_DURATION_MS);
      }
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Returning with success: " + _success + " and error messsage: " + _errorMessage);

  }

  // Inputs setters

  @Input(required = Required.OPTIONAL)
  @Name("SecureCredentialsStoreKey")
  public void setSecureCredentialsStoreKey(String val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Secure Credentials Store Key, value: " + val);

    _storeKey = val;
  }

  @Input(required = Required.ALWAYS)
  @Name("Servers")
  public void setServers(String servers) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Servers, value: " + servers);
    _servers = servers;
  }

  @Input(required = Required.ALWAYS)
  public void setGroupId(String groupId) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Group Id, value: " + groupId);
    _groupId = groupId;
  }

  @Input(required = Required.ALWAYS)
  public void setTopic(String topic) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Topic, value: " + topic);
    _topic = topic;
  }

  @Input(required = Required.ALWAYS)
  public void setJobTypeId(Integer val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Job Type Id, value: " + val);
    _jobTypeId = val;
  }

  @Input(required = Required.OPTIONAL, defaultValue = "" + SESSION_TIMEOUT_MS)
  public void setSessionTimeout(Integer ms) {
    if (ms == null) {
      _sessionTimeoutMs = SESSION_TIMEOUT_MS;
    } else {
      _sessionTimeoutMs = ms;
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Input SessionTimeout, value: " + ms);
  }

  @Input(required = Required.ALWAYS, enumeration = "kafkaSecurityProtocols")
  public void setSecurityProtocol(String securityProtocol) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input securityProtocol, value: " + securityProtocol);
    _securityProtocol = securityProtocol;
  }

  @Input(required = Required.ALWAYS)
  public void setPollingIntervalInMs(Integer ms) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Polling Interval, value: " + ms);

    _pollingIntervalMs = ms;
  }

  @Input(required = Required.ALWAYS, defaultValue = "1")
  public void setNumConsumers(Integer num) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Number Of Consumer in Group, value: " + num);

    _numConsumers = num;
  }

  @Input(required = Required.ALWAYS, defaultValue = AppianKafkaUtil.TRANSACTION_TABLE_NAME)
  public void setTransactionTableName(String name) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Transaction Table Name, value: " + name);

    _transactionTableName = name;
  }

  @Input(required = Required.ALWAYS)
  public void setSleepingIntervalInMs(Integer ms) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Sleeping Interval, value: " + ms);
    _sleepingIntervalMs = ms;
  }

  @Input(required = Required.ALWAYS)
  public void setRuntimeInMinutes(Integer val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Runtime in Minutes, value: " + val);
    _runtimeInMinutes = val;
  }

  @Input(required = Required.OPTIONAL, enumeration = "kafkaSaslMechanism")
  public void setSASLMechanism(String saslMechanism) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input SASLMechanism value: " + saslMechanism);

    _saslMechanism = saslMechanism;
  }

  @Input(required = Required.OPTIONAL)
  public void setDeserializerClassName(String val) {
    if (StringUtils.isBlank(val)) {
      _deserializerClassName = DESERIALIZER_CLASS_NAME;
    } else {
      _deserializerClassName = val;
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Input Deserializer Class Name value: " + val);
  }

  @Input(required = Required.OPTIONAL)
  @DocumentDataType
  public void setTrustStore(Long val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input TrustStore value: " + val);
    _trustStoreDoc = val;
  }

  @Input(required = Required.OPTIONAL)
  @DocumentDataType
  public void setKeyStore(Long val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input KeyStore value: " + val);
    _keyStoreDoc = val;
  }

  @Input(required = Required.ALWAYS)
  public void setDataSourceName(String val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input DateSourceName value: " + val);
    _dsName = val;
  }

  @Input(required = Required.OPTIONAL)
  public void setMessageFilter(String val) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input Query Filter value: " + val);
    _messageFilter = val;
  }

  // Outputs getters
  @Name("Success")
  public Boolean getSuccess() {
    return _success;
  }

  @Name("ErrorMessage")
  public String getErrorMessage() {
    return _errorMessage;
  }

}
