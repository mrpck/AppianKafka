package com.appiancorp.cs.plugin.kafka.smartservice;

import java.util.Properties;

import javax.naming.Context;

import org.apache.log4j.Logger;

import com.appiancorp.cs.plugin.kafka.AppianKafkaUtil;
import com.appiancorp.cs.plugin.kafka.producer.AppianKafkaProducer;
import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.content.ContentService;
import com.appiancorp.suiteapi.knowledge.DocumentDataType;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.MessageContainer;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;

@PaletteInfo(paletteCategory = "Integration Services", palette = "Connectivity Services")
@Order({
  // inputs
  "SecureCredentialsStoreKey", "Servers", "Topic", "Payload", "SecurityProtocol", "SASLMechanism", "Truststore", "Keystore",

  // outputs
  "Success", "ErrorMessage"
})

// I think that in the future - and depending on the volume - we may want to keep the producers opened and "cache" them into a static map.
// Creating a producer for each message is going to be very inefficient. Producers are thread safe so we should be fine reusing them.
// More research and reading required

public class PublishKafkaSmartService extends AppianSmartService {

  private static final Logger LOG = Logger.getLogger(PublishKafkaSmartService.class);

  private final SecureCredentialsStore _scs;
  private final ContentService _cs;
  private final Context _ctx;
  private final Properties _props = new Properties();

  private static final String SERIALIZER_CLASS_NAME = "org.apache.kafka.common.serialization.StringSerializer";

  // Inputs
  private String _storeKey;
  private String _servers;
  private String _topic;
  private String _securityProtocol;
  private String _saslMechanism;
  private Long _trustStoreDoc;
  private Long _keyStoreDoc;
  private String _payload;

  // Outputs
  private Boolean _success;
  private String _errorMessage;

  public PublishKafkaSmartService(SecureCredentialsStore scs, ContentService cs, Context ctx) {
    super();
    if (LOG.isDebugEnabled())
      LOG.debug("DEBUG: instantiating");

    this._scs = scs;
    this._cs = cs;
    this._ctx = ctx;
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
  }

  @Override
  public void run() throws SmartServiceException {
    try {
      AppianKafkaUtil.setProperties(_scs, _cs, _props, _servers, _securityProtocol, _saslMechanism, _storeKey, null,
        SERIALIZER_CLASS_NAME, SERIALIZER_CLASS_NAME, null, _trustStoreDoc, _keyStoreDoc, false);

      AppianKafkaProducer kafkaProducer = new AppianKafkaProducer(_props);
      kafkaProducer.publish(_topic, _payload);

      _success = kafkaProducer.getSuccess();
      _errorMessage = kafkaProducer.getException();

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      _success = false;
      _errorMessage = e.getMessage();
    }
  }

  // Inputs setters
  @Input(required = Required.ALWAYS)
  @Name("SecureCredentialsStoreKey")
  public void setSecureCredentialsStoreKey(String val) {
    _storeKey = val;
  }

  @Input(required = Required.ALWAYS)
  @Name("Servers")
  public void setServers(String servers) {
    _servers = servers;
  }

  @Input(required = Required.ALWAYS)
  public void setTopic(String topic) {
    _topic = topic;
  }

  @Input(required = Required.ALWAYS)
  public void setSecurityProtocol(String securityProtocol) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input securityProtocol, value: " + securityProtocol);
    _securityProtocol = securityProtocol;
  }

  @Input(required = Required.OPTIONAL)
  public void setSASLMechanism(String saslMechanism) {
    if (LOG.isDebugEnabled())
      LOG.debug("Input saslMechanism, value: " + saslMechanism);
    _saslMechanism = saslMechanism;
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
  public void setPayload(String val) {
    this._payload = val;
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
