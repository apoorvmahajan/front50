/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.front50.controllers;

import static com.netflix.spinnaker.front50.model.pipeline.Pipeline.TYPE_TEMPLATED;
import static com.netflix.spinnaker.front50.model.pipeline.TemplateConfiguration.TemplateSource.SPINNAKER_PREFIX;
import static java.lang.String.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.spinnaker.fiat.shared.FiatPermissionEvaluator;
import com.netflix.spinnaker.front50.ServiceAccountsService;
import com.netflix.spinnaker.front50.config.controllers.PipelineControllerConfig;
import com.netflix.spinnaker.front50.exception.BadRequestException;
import com.netflix.spinnaker.front50.exceptions.DuplicateEntityException;
import com.netflix.spinnaker.front50.exceptions.InvalidEntityException;
import com.netflix.spinnaker.front50.exceptions.InvalidRequestException;
import com.netflix.spinnaker.front50.model.pipeline.Pipeline;
import com.netflix.spinnaker.front50.model.pipeline.PipelineDAO;
import com.netflix.spinnaker.front50.model.pipeline.PipelineTemplateDAO;
import com.netflix.spinnaker.front50.model.pipeline.TemplateConfiguration;
import com.netflix.spinnaker.front50.model.pipeline.V2TemplateConfiguration;
import com.netflix.spinnaker.front50.validator.GenericValidationErrors;
import com.netflix.spinnaker.front50.validator.PipelineValidator;
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException;
import com.netflix.spinnaker.kork.web.exceptions.ValidationException;
import com.netflix.spinnaker.security.AuthenticatedRequest;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Controller for presets */
@RestController
@RequestMapping("pipelines")
public class PipelineController {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final PipelineDAO pipelineDAO;
  private final ObjectMapper objectMapper;
  private final Optional<ServiceAccountsService> serviceAccountsService;
  private final List<PipelineValidator> pipelineValidators;
  private final Optional<PipelineTemplateDAO> pipelineTemplateDAO;
  private final PipelineControllerConfig pipelineControllerConfig;
  private final FiatPermissionEvaluator fiatPermissionEvaluator;

  public PipelineController(
      PipelineDAO pipelineDAO,
      ObjectMapper objectMapper,
      Optional<ServiceAccountsService> serviceAccountsService,
      List<PipelineValidator> pipelineValidators,
      Optional<PipelineTemplateDAO> pipelineTemplateDAO,
      PipelineControllerConfig pipelineControllerConfig,
      FiatPermissionEvaluator fiatPermissionEvaluator) {
    this.pipelineDAO = pipelineDAO;
    this.objectMapper = objectMapper;
    this.serviceAccountsService = serviceAccountsService;
    this.pipelineValidators = pipelineValidators;
    this.pipelineTemplateDAO = pipelineTemplateDAO;
    this.pipelineControllerConfig = pipelineControllerConfig;
    this.fiatPermissionEvaluator = fiatPermissionEvaluator;
  }

  @PreAuthorize("#restricted ? @fiatPermissionEvaluator.storeWholePermission() : true")
  @PostFilter("#restricted ? hasPermission(filterObject.name, 'APPLICATION', 'READ') : true")
  @RequestMapping(value = "", method = RequestMethod.GET)
  public Collection<Pipeline> list(
      @RequestParam(required = false, value = "restricted", defaultValue = "true")
          boolean restricted,
      @RequestParam(required = false, value = "refresh", defaultValue = "true") boolean refresh) {
    return pipelineDAO.all(refresh);
  }

  @PreAuthorize("hasPermission(#application, 'APPLICATION', 'READ')")
  @RequestMapping(value = "{application:.+}", method = RequestMethod.GET)
  public List<Pipeline> listByApplication(
      @PathVariable(value = "application") String application,
      @RequestParam(required = false, value = "refresh", defaultValue = "true") boolean refresh) {
    List<Pipeline> pipelines =
        new ArrayList<>(pipelineDAO.getPipelinesByApplication(application, refresh));

    pipelines.sort(
        (p1, p2) -> {
          if (p1.getIndex() != null && p2.getIndex() == null) {
            return -1;
          }
          if (p1.getIndex() == null && p2.getIndex() != null) {
            return 1;
          }
          if (p1.getIndex() != null
              && p2.getIndex() != null
              && !p1.getIndex().equals(p2.getIndex())) {
            return p1.getIndex() - p2.getIndex();
          }
          return Optional.ofNullable(p1.getName())
              .orElse(p1.getId())
              .compareToIgnoreCase(Optional.ofNullable(p2.getName()).orElse(p2.getId()));
        });

    int i = 0;
    for (Pipeline p : pipelines) {
      p.setIndex(i);
      i++;
    }

    return pipelines;
  }

  @PreAuthorize("@fiatPermissionEvaluator.storeWholePermission()")
  @PostFilter("hasPermission(filterObject.application, 'APPLICATION', 'READ')")
  @RequestMapping(value = "{id:.+}/history", method = RequestMethod.GET)
  public Collection<Pipeline> getHistory(
      @PathVariable String id, @RequestParam(value = "limit", defaultValue = "20") int limit) {
    return pipelineDAO.history(id, limit);
  }

  @PreAuthorize("@fiatPermissionEvaluator.storeWholePermission()")
  @PostAuthorize("hasPermission(returnObject.application, 'APPLICATION', 'READ')")
  @RequestMapping(value = "{id:.+}/get", method = RequestMethod.GET)
  public Pipeline get(@PathVariable String id) {
    return pipelineDAO.findById(id);
  }

  @PreAuthorize(
      "@fiatPermissionEvaluator.storeWholePermission() "
          + "and hasPermission(#pipeline.application, 'APPLICATION', 'WRITE') "
          + "and @authorizationSupport.hasRunAsUserPermission(#pipeline)")
  @RequestMapping(value = "", method = RequestMethod.POST)
  public Pipeline save(
      @RequestBody Pipeline pipeline,
      @RequestParam(value = "staleCheck", required = false, defaultValue = "false")
          Boolean staleCheck) {

    long saveStartTime = System.currentTimeMillis();
    log.info(
        "Received request to save pipeline {} in application {}",
        pipeline.getName(),
        pipeline.getApplication());

    log.debug("Running validation before saving pipeline {}", pipeline.getName());
    long validationStartTime = System.currentTimeMillis();
    validatePipeline(pipeline, staleCheck);
    checkForDuplicatePipeline(
        pipeline.getApplication(), pipeline.getName().trim(), pipeline.getId());
    pipeline.setName(pipeline.getName().trim());
    log.debug(
        "Successfully validated pipeline {} in {}ms",
        pipeline.getName(),
        System.currentTimeMillis() - validationStartTime);

    Pipeline savedPipeline = pipelineDAO.create(pipeline.getId(), pipeline);
    log.info(
        "Successfully saved pipeline {} in application {} in {}ms",
        savedPipeline.getName(),
        savedPipeline.getApplication(),
        System.currentTimeMillis() - saveStartTime);
    return savedPipeline;
  }

  @PreAuthorize(
      "@fiatPermissionEvaluator.storeWholePermission() "
          + "and @authorizationSupport.hasRunAsUserPermission(#pipelines)")
  @RequestMapping(value = "batchUpdate", method = RequestMethod.POST)
  public Map<String, Object> batchUpdate(
      @RequestBody List<Pipeline> pipelines,
      @RequestParam(value = "staleCheck", required = false, defaultValue = "false")
          Boolean staleCheck) {

    long batchUpdateStartTime = System.currentTimeMillis();

    Map<String, Object> returnData = new HashMap<>();

    log.info(
        "Received request to batch update the following pipelines: {}",
        pipelines.stream().map(Pipeline::getName).collect(Collectors.toList()));

    log.debug("Following pipeline definitions will be processed: {}", pipelines);

    List<Pipeline> pipelinesToSave = new ArrayList<>();
    List<Pipeline> invalidPipelines = new ArrayList<>();

    validatePipelines(pipelines, staleCheck, pipelinesToSave, invalidPipelines);

    long bulkImportStartTime = System.currentTimeMillis();
    log.debug("Bulk importing the following pipelines: {}", pipelinesToSave);
    pipelineDAO.bulkImport(pipelinesToSave);
    log.debug(
        "Bulk imported {} pipelines successfully in {}ms",
        pipelinesToSave.size(),
        System.currentTimeMillis() - bulkImportStartTime);

    List<String> savedPipelines =
        pipelinesToSave.stream().map(Pipeline::getName).collect(Collectors.toList());
    returnData.put("successful_pipelines_count", savedPipelines.size());
    returnData.put("successful_pipelines", savedPipelines);
    returnData.put("failed_pipelines_count", invalidPipelines.size());
    returnData.put("failed_pipelines", invalidPipelines);

    if (!invalidPipelines.isEmpty()) {
      log.warn(
          "Following pipelines were skipped during the bulk import since they had errors: {}",
          invalidPipelines.stream().map(Pipeline::getName).collect(Collectors.toList()));
    }
    log.info(
        "Batch updated the following {} pipelines in {}ms: {}",
        savedPipelines.size(),
        System.currentTimeMillis() - batchUpdateStartTime,
        savedPipelines);

    return returnData;
  }

  @PreAuthorize("hasPermission(#application, 'APPLICATION', 'WRITE')")
  @RequestMapping(value = "{application}/{pipeline:.+}", method = RequestMethod.DELETE)
  public void delete(@PathVariable String application, @PathVariable String pipeline) {
    String pipelineId = pipelineDAO.getPipelineId(application, pipeline);
    log.info(
        "Deleting pipeline \"{}\" with id {} in application {}", pipeline, pipelineId, application);
    pipelineDAO.delete(pipelineId);

    serviceAccountsService.ifPresent(
        accountsService ->
            accountsService.deleteManagedServiceAccounts(Collections.singletonList(pipelineId)));
  }

  public void delete(@PathVariable String id) {
    pipelineDAO.delete(id);
    serviceAccountsService.ifPresent(
        accountsService ->
            accountsService.deleteManagedServiceAccounts(Collections.singletonList(id)));
  }

  @PreAuthorize("hasPermission(#pipeline.application, 'APPLICATION', 'WRITE')")
  @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
  public Pipeline update(
      @PathVariable final String id,
      @RequestParam(value = "staleCheck", required = false, defaultValue = "false")
          Boolean staleCheck,
      @RequestBody Pipeline pipeline) {
    Pipeline existingPipeline = pipelineDAO.findById(id);

    if (!pipeline.getId().equals(existingPipeline.getId())) {
      throw new InvalidRequestException(
          format(
              "The provided id %s doesn't match the existing pipeline id %s",
              pipeline.getId(), existingPipeline.getId()));
    }

    validatePipeline(pipeline, staleCheck);
    checkForDuplicatePipeline(
        pipeline.getApplication(), pipeline.getName().trim(), pipeline.getId());

    pipeline.put("updateTs", System.currentTimeMillis());

    pipelineDAO.update(id, pipeline);

    return pipeline;
  }

  /**
   * Ensure basic validity of the pipeline. Invalid pipelines will raise runtime exceptions.
   *
   * @param pipeline The Pipeline to validate
   */
  private void validatePipeline(final Pipeline pipeline, Boolean staleCheck) {
    // Check if valid pipeline name and app name have been provided
    if (StringUtils.isAnyBlank(pipeline.getApplication(), pipeline.getName())) {
      throw new InvalidEntityException(
          "Invalid pipeline definition provided. A valid pipeline name and application name must be provided.");
    }
    pipeline.setName(pipeline.getName().trim());

    // Check if pipeline type is templated
    if (TYPE_TEMPLATED.equals(pipeline.getType())) {
      PipelineTemplateDAO templateDAO = getTemplateDAO();

      // Check templated pipelines to ensure template is valid
      String source;
      switch (pipeline.getSchema()) {
        case "v2":
          V2TemplateConfiguration v2Config =
              objectMapper.convertValue(pipeline, V2TemplateConfiguration.class);
          source = v2Config.getTemplate().getReference();
          break;
        default:
          TemplateConfiguration v1Config =
              objectMapper.convertValue(pipeline.getConfig(), TemplateConfiguration.class);
          source = v1Config.getPipeline().getTemplate().getSource();
          break;
      }

      // With the source check if it starts with "spinnaker://"
      // Check if template id which is after :// is in the store
      if (source.startsWith(SPINNAKER_PREFIX)) {
        String templateId = source.substring(SPINNAKER_PREFIX.length());
        try {
          templateDAO.findById(templateId);
        } catch (NotFoundException notFoundEx) {
          throw new BadRequestException("Configured pipeline template not found", notFoundEx);
        }
      }
    }

    ensureCronTriggersHaveIdentifier(pipeline);

    // Ensure cron trigger ids are regenerated if needed
    if (Strings.isNullOrEmpty(pipeline.getId())
        || (boolean) pipeline.getOrDefault("regenerateCronTriggerIds", false)) {
      // ensure that cron triggers are assigned a unique identifier for new pipelines
      pipeline.setTriggers(
          pipeline.getTriggers().stream()
              .map(
                  it -> {
                    if ("cron".equalsIgnoreCase(it.getType())) {
                      it.put("id", UUID.randomUUID().toString());
                    }
                    return it;
                  })
              .collect(Collectors.toList()));
    }

    final GenericValidationErrors errors = new GenericValidationErrors(pipeline);

    // Run stale pipeline definition check
    if (staleCheck
        && !Strings.isNullOrEmpty(pipeline.getId())
        && pipeline.getLastModified() != null) {
      checkForStalePipeline(pipeline, errors);
    }

    // Run other pre-configured validators
    pipelineValidators.forEach(it -> it.validate(pipeline, errors));

    if (errors.hasErrors()) {
      List<String> message =
          errors.getAllErrors().stream()
              .map(DefaultMessageSourceResolvable::getDefaultMessage)
              .collect(Collectors.toList());
      throw new ValidationException(message);
    }
  }

  private PipelineTemplateDAO getTemplateDAO() {
    return pipelineTemplateDAO.orElseThrow(
        () ->
            new BadRequestException(
                "Pipeline Templates are not supported with your current storage backend"));
  }

  private void checkForStalePipeline(Pipeline pipeline, GenericValidationErrors errors) {
    Pipeline existingPipeline = pipelineDAO.findById(pipeline.getId());
    Long storedUpdateTs = existingPipeline.getLastModified();
    Long submittedUpdateTs = pipeline.getLastModified();
    if (!submittedUpdateTs.equals(storedUpdateTs)) {
      errors.reject(
          "stale",
          "The submitted pipeline is stale.  submitted updateTs "
              + submittedUpdateTs
              + " does not match stored updateTs "
              + storedUpdateTs);
    }
  }

  private void checkForDuplicatePipeline(String application, String name, String id) {
    log.debug(
        "Cache refresh enabled when checking for duplicates: {}",
        pipelineControllerConfig.getSave().isRefreshCacheOnDuplicatesCheck());
    boolean any =
        pipelineDAO
            .getPipelinesByApplication(
                application, pipelineControllerConfig.getSave().isRefreshCacheOnDuplicatesCheck())
            .stream()
            .anyMatch(it -> it.getName().equalsIgnoreCase(name) && !it.getId().equals(id));
    if (any) {
      throw new DuplicateEntityException(
          format("A pipeline with name %s already exists in application %s", name, application));
    }
  }

  private static void ensureCronTriggersHaveIdentifier(Pipeline pipeline) {
    // ensure that all cron triggers have an assigned identifier
    pipeline.setTriggers(
        pipeline.getTriggers().stream()
            .map(
                it -> {
                  if ("cron".equalsIgnoreCase(it.getType())) {
                    String triggerId = (String) it.getOrDefault("id", "");
                    if (triggerId.isEmpty()) {
                      it.put("id", UUID.randomUUID().toString());
                    }
                  }
                  return it;
                })
            .collect(Collectors.toList()));
  }

  /** * Fetches all the pipelines and groups then into a map indexed by applications */
  private Map<String, List<Pipeline>> getAllPipelinesByApplication() {
    Map<String, List<Pipeline>> appToPipelines = new HashMap<>();
    pipelineDAO
        .all(false)
        .forEach(
            pipeline ->
                appToPipelines
                    .computeIfAbsent(pipeline.getApplication(), k -> new ArrayList<>())
                    .add(pipeline));
    return appToPipelines;
  }

  /**
   * Validates the provided list of pipelines and populates the provided valid and invalid pipelines
   * accordingly. Following validations are performed:
   *
   * @param pipelines List of {@link Pipeline} to be validated
   * @param staleCheck Controls whether stale check should be performed while validating pipelines
   * @param validPipelines Result list of {@link Pipeline} that passed validations
   * @param invalidPipelines Result list of {@link Pipeline} that failed validations
   */
  private void validatePipelines(
      List<Pipeline> pipelines,
      Boolean staleCheck,
      List<Pipeline> validPipelines,
      List<Pipeline> invalidPipelines) {

    Map<String, List<Pipeline>> pipelinesByApp = getAllPipelinesByApplication();
    Map<String, Boolean> appPermissionForUser = new HashMap<>();
    Set<String> uniqueIdSet = new HashSet<>();

    final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    String user = AuthenticatedRequest.getSpinnakerUser().orElse("anonymous");

    long validationStartTime = System.currentTimeMillis();
    log.debug("Running validations before saving");
    for (Pipeline pipeline : pipelines) {
      try {
        validatePipeline(pipeline, staleCheck);

        String app = pipeline.getApplication();
        String pipelineName = pipeline.getName();

        // Check if user has permission to write the pipeline
        if (!appPermissionForUser.computeIfAbsent(
            app,
            key ->
                fiatPermissionEvaluator.hasPermission(
                    auth, pipeline.getApplication(), "APPLICATION", "WRITE"))) {
          String errorMessage =
              String.format(
                  "User %s does not have WRITE permission to save the pipeline %s in the application %s.",
                  user, pipeline.getName(), pipeline.getApplication());
          log.error(errorMessage);
          pipeline.put("errorMsg", errorMessage);
          invalidPipelines.add(pipeline);
          continue;
        }

        // Check if duplicate pipeline exists in the same app
        List<Pipeline> appPipelines = pipelinesByApp.getOrDefault(app, new ArrayList<>());
        if (appPipelines.stream()
            .anyMatch(
                existingPipeline ->
                    existingPipeline.getName().equalsIgnoreCase(pipeline.getName())
                        && !existingPipeline.getId().equals(pipeline.getId()))) {
          String errorMessage =
              String.format(
                  "A pipeline with name %s already exists in the application %s",
                  pipelineName, app);
          log.error(errorMessage);
          pipeline.put("errorMsg", errorMessage);
          invalidPipelines.add(pipeline);
          continue;
        }

        // Validate pipeline id
        String id = pipeline.getId();
        if (Strings.isNullOrEmpty(id)) {
          pipeline.setId(UUID.randomUUID().toString());
        } else if (!uniqueIdSet.add(id)) {
          String errorMessage =
              String.format(
                  "Duplicate pipeline id %s found when processing pipeline %s in the application %s",
                  id, pipeline.getName(), pipeline.getApplication());
          log.error(errorMessage);
          invalidPipelines.add(pipeline);
          pipeline.put("errorMsg", errorMessage);
          continue;
        }
        validPipelines.add(pipeline);
      } catch (Exception e) {
        String errorMessage =
            String.format(
                "Encountered the following error when validating pipeline %s in the application %s: %s",
                pipeline.getName(), pipeline.getApplication(), e.getMessage());
        log.error(errorMessage, e);
        pipeline.put("errorMsg", errorMessage);
        invalidPipelines.add(pipeline);
      }
    }
    log.debug(
        "Validated {} pipelines in {}ms",
        pipelines.size(),
        System.currentTimeMillis() - validationStartTime);
  }
}
