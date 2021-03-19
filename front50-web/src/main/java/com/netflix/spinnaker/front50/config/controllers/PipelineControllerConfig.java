package com.netflix.spinnaker.front50.config.controllers;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "controller.pipeline")
public class PipelineControllerConfig {

  /** Holds the configurations to be used for save/update controller mappings */
  private SavePipelineConfiguration save = new SavePipelineConfiguration();

  @Data
  public static class SavePipelineConfiguration {
    /** This controls whether cache should be refreshes while checking for duplicate pipelines */
    boolean refreshCacheOnDuplicatesCheck = true;
  }
}
