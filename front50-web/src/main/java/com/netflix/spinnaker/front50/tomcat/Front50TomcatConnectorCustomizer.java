package com.netflix.spinnaker.front50.tomcat;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;

/**
 * Front50-specific tomcat customzations.
 *
 * <p>The plan is eventually to make this change directly in kork (see
 * https://github.com/spinnaker/kork/blob/v7.78.0/kork-tomcat/src/main/java/com/netflix/spinnaker/kork/tomcat/DefaultTomcatConnectorCustomizer.java#47),
 * at which point this class can disappear.
 */
public class Front50TomcatConnectorCustomizer implements TomcatConnectorCustomizer {

  private final Front50TomcatConfigurationProperties front50TomcatConfigurationProperties;

  public Front50TomcatConnectorCustomizer(
      Front50TomcatConfigurationProperties front50TomcatConfigurationProperties) {
    this.front50TomcatConfigurationProperties = front50TomcatConfigurationProperties;
  }

  @Override
  public void customize(Connector connector) {
    if (front50TomcatConfigurationProperties.getRejectIllegalHeader() != null) {
      ((AbstractHttp11Protocol<?>) connector.getProtocolHandler())
          .setRejectIllegalHeader(front50TomcatConfigurationProperties.getRejectIllegalHeader());
    }
  }
}
