package com.netflix.spinnaker.front50.tomcat;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Front50-specific tomcat customization properties
 *
 * <p>The choice of front50.tomcat isn't ideal, given that we'd like to make this change directly in
 * kork eventually. kork uses "default" for it's DefaultTomcatConfigurationProperties class (see
 * e.g.
 * https://github.com/spinnaker/kork/blob/v7.78.0/kork-tomcat/src/main/java/com/netflix/spinnaker/kork/tomcat/TomcatConfigurationProperties.java#L25),
 * but using the same @ConfigurationProperties("default") annotation in two places feels risky.
 */
@Data
@ConfigurationProperties("front50.tomcat")
public class Front50TomcatConfigurationProperties {

  private Boolean rejectIllegalHeader;
}
