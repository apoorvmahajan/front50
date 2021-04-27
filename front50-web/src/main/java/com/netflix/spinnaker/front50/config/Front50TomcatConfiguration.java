package com.netflix.spinnaker.front50.config;

import com.netflix.spinnaker.front50.tomcat.Front50TomcatConfigurationProperties;
import com.netflix.spinnaker.front50.tomcat.Front50TomcatConnectorCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class exists allow front50-specific customizations to tomcat, e.g. setting tomcat's
 * setRejectIllegalHeader property. The plan is to make this change directly in kork, at which point
 * this class can disappear.
 */
@Configuration
@EnableConfigurationProperties({Front50TomcatConfigurationProperties.class})
class Front50TomcatConfiguration {

  /** Configure tomcat to allow front50-specific customizations. */
  @Bean
  TomcatConnectorCustomizer front50TomcatConnectorCustomizer(
      Front50TomcatConfigurationProperties front50TomcatConfigurationProperties) {
    return new Front50TomcatConnectorCustomizer(front50TomcatConfigurationProperties);
  }
}
