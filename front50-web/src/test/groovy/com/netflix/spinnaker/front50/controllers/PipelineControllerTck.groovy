/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.controllers

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.fiat.shared.FiatPermissionEvaluator
import com.netflix.spinnaker.front50.ServiceAccountsService
import com.netflix.spinnaker.front50.config.StorageServiceConfigurationProperties
import com.netflix.spinnaker.front50.model.DefaultObjectKeyLoader
import com.netflix.spinnaker.front50.model.S3StorageService
import com.netflix.spinnaker.front50.model.SqlStorageService
import com.netflix.spinnaker.front50.model.pipeline.DefaultPipelineDAO
import com.netflix.spinnaker.kork.api.exceptions.ExceptionMessage
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import com.netflix.spinnaker.kork.web.exceptions.ExceptionMessageDecorator
import com.netflix.spinnaker.kork.web.exceptions.GenericExceptionHandlers
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.assertj.core.util.Lists
import org.springframework.beans.factory.ObjectProvider

import java.time.Clock
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.s3.AmazonS3Client
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.front50.model.pipeline.Pipeline
import com.netflix.spinnaker.front50.model.pipeline.PipelineDAO
import com.netflix.spinnaker.front50.utils.S3TestHelper
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import rx.schedulers.Schedulers
import spock.lang.*
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status


abstract class PipelineControllerTck extends Specification {

  static final int OK = 200
  static final int BAD_REQUEST = 400
  static final int UNPROCESSABLE_ENTITY = 422

  MockMvc mockMvc

  @Subject
  PipelineDAO pipelineDAO
  ServiceAccountsService serviceAccountsService
  StorageServiceConfigurationProperties.PerObjectType pipelineDAOConfigProperties =
    new StorageServiceConfigurationProperties().getPipeline()
  FiatPermissionEvaluator fiatPermissionEvaluator


  void setup() {
    this.pipelineDAO = Spy(createPipelineDAO())
    this.serviceAccountsService = Mock(ServiceAccountsService)
    this.fiatPermissionEvaluator = Mock(FiatPermissionEvaluator)

    mockMvc = MockMvcBuilders
      .standaloneSetup(new PipelineController(pipelineDAO,
        new ObjectMapper(),
        Optional.of(serviceAccountsService),
        Lists.emptyList(),
        Optional.empty(),
        fiatPermissionEvaluator))
      .setControllerAdvice(
        new GenericExceptionHandlers(
          new ExceptionMessageDecorator(Mock(ObjectProvider) as ObjectProvider<List<ExceptionMessage>>)
        )
      )
      .build()
  }

  abstract PipelineDAO createPipelineDAO()

  def "should fail to save if application is missing"() {
    given:
    def command = [
      name: "some pipeline with no application"
    ]

    when:
    def response = mockMvc
      .perform(
        post("/pipelines")
          .contentType(MediaType.APPLICATION_JSON)
          .content(new ObjectMapper().writeValueAsString(command))
      )
      .andReturn()
      .response

    then:
    response.status == UNPROCESSABLE_ENTITY
  }

  void "should provide a valid, unique index when listing all for an application"() {
    given:
    pipelineDAO.create(null, new Pipeline([
      name: "c", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a3", application: "test", index: 3
    ]))

    when:
    def response = mockMvc.perform(get("/pipelines/test"))

    then:
    response
      .andExpect(jsonPath('$.[*].name').value(["a1", "b1", "a3", "b", "c"]))
      .andExpect(jsonPath('$.[*].index').value([0, 1, 2, 3, 4]))
  }

  void 'should update a pipeline'() {
    given:
    def pipeline = pipelineDAO.create(null, new Pipeline([name: "test pipeline", application: "test_application"]))

    when:
    pipeline.name = "Updated Name"
    def response = mockMvc.perform(put("/pipelines/${pipeline.id}").contentType(MediaType.APPLICATION_JSON)
      .content(new ObjectMapper().writeValueAsString(pipeline))).andReturn().response

    then:
    response.status == OK
    pipelineDAO.findById(pipeline.getId()).getName() == "Updated Name"

  }

  void 'should fail update on duplicate pipeline or invalid request'() {
    given:
    def (pipeline1, pipeline2) = [
      pipelineDAO.create(null, new Pipeline([name: "test pipeline 1", application: "test_application"])),
      pipelineDAO.create(null, new Pipeline([name: "test pipeline 2", application: "test_application"]))
    ]

    and:
    pipeline1.name = pipeline2.name

    when:
    def response = mockMvc.perform(put("/pipelines/${pipeline1.id}").contentType(MediaType.APPLICATION_JSON)
      .content(new ObjectMapper().writeValueAsString(pipeline1))).andReturn().response

    then:
    response.status == BAD_REQUEST
    response.errorMessage == "A pipeline with name ${pipeline2.name} already exists in application ${pipeline2.application}"

    when:
    response = mockMvc.perform(put("/pipelines/${pipeline2.id}").contentType(MediaType.APPLICATION_JSON)
      .content(new ObjectMapper().writeValueAsString(pipeline1))).andReturn().response

    then:
    response.status == BAD_REQUEST
    response.errorMessage == "The provided id ${pipeline1.id} doesn't match the existing pipeline id ${pipeline2.id}"
  }

  @Unroll
  @Ignore("Fix test with the bulk save change")
  void 'should only (re)generate cron trigger ids for new pipelines'() {
    given:
    def pipeline = [
      name       : "My Pipeline",
      application: "test",
      triggers   : [
        [type: "cron", id: "original-id"]
      ]
    ]
    if (lookupPipelineId) {
      pipelineDAO.create(null, pipeline as Pipeline)
      pipeline.id = pipelineDAO.findById(
        pipelineDAO.getPipelineId("test", "My Pipeline")
      ).getId()
    }

    when:
    def response = mockMvc.perform(post('/pipelines').
      contentType(MediaType.APPLICATION_JSON).content(new ObjectMapper().writeValueAsString(pipeline)))
      .andReturn().response

    def updatedPipeline = pipelineDAO.findById(
      pipelineDAO.getPipelineId("test", "My Pipeline")
    )

    then:
    response.status == OK
    expectedTriggerCheck.call(updatedPipeline)

    where:
    lookupPipelineId || expectedTriggerCheck
    false            || { Map p -> p.triggers*.id != ["original-id"] }
    true             || { Map p -> p.triggers*.id == ["original-id"] }
  }

  @Ignore("Fix test with the bulk save change")
  void 'should ensure that all cron triggers have an identifier'() {
    given:
    def pipeline = [
      name       : "My Pipeline",
      application: "test",
      triggers   : [
        [type: "cron", id: "original-id", expression: "1"],
        [type: "cron", expression: "2"],
        [type: "cron", id: "", expression: "3"]
      ]
    ]

    pipelineDAO.create(null, pipeline as Pipeline)
    pipeline.id = pipelineDAO.findById(
      pipelineDAO.getPipelineId("test", "My Pipeline")
    ).getId()

    when:
    def response = mockMvc.perform(post('/pipelines').
      contentType(MediaType.APPLICATION_JSON).content(new ObjectMapper().writeValueAsString(pipeline)))
      .andReturn().response

    def updatedPipeline = pipelineDAO.findById(
      pipelineDAO.getPipelineId("test", "My Pipeline")
    )

    then:
    response.status == OK
    updatedPipeline.get("triggers").find { it.expression == "1" }.id == "original-id"
    updatedPipeline.get("triggers").find { it.expression == "2" }.id.length() > 1
    updatedPipeline.get("triggers").find { it.expression == "3" }.id.length() > 1
  }

  void 'should delete an existing pipeline by name or id and its associated managed service account'() {
    given:
    def pipelineToDelete = pipelineDAO.create(null, new Pipeline([
      name: "pipeline1", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "pipeline2", application: "test"
    ]))

    when:
    def allPipelines = pipelineDAO.all()
    def allPipelinesForApplication = pipelineDAO.getPipelinesByApplication("test")

    then:
    allPipelines*.id.sort() == allPipelinesForApplication*.id.sort()
    allPipelines.size() == 2

    when:
    def response = mockMvc.perform(delete('/pipelines/test/pipeline1')).andReturn().response

    then:
    response.status == OK
    pipelineDAO.all()*.name == ["pipeline2"]
    1 * serviceAccountsService.deleteManagedServiceAccounts([pipelineToDelete.id])
  }

  void 'should enforce unique names on save operations'() {
    given:
    pipelineDAO.create(null, new Pipeline([
      name: "pipeline1", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "pipeline2", application: "test"
    ]))

    when:
    def allPipelines = pipelineDAO.all()
    def allPipelinesForApplication = pipelineDAO.getPipelinesByApplication("test")

    then:
    allPipelines*.id.sort() == allPipelinesForApplication*.id.sort()
    allPipelines.size() == 2

    when:
    def response = mockMvc.perform(post('/pipelines')
      .contentType(MediaType.APPLICATION_JSON)
      .content(new ObjectMapper().writeValueAsString([name: "pipeline1", application: "test"])))
      .andReturn().response

    then:
    response.status == BAD_REQUEST
    response.errorMessage == "A pipeline with name pipeline1 already exists in application test"
  }

  def "multi-threaded cache refresh with no synchronization"() {
    given:
    pipelineDAO.create(null, new Pipeline([
      name: "c", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a3", application: "test", index: 3
    ]))

    when:
    def results = new ArrayList(10)
    def futures = new ArrayList(10)

    def threadPool = Executors.newFixedThreadPool(10)
    try {
      10.times {

        futures.add(threadPool.submit({ ->
          def mockMvcTest = MockMvcBuilders
            .standaloneSetup(new PipelineController(pipelineDAO,
              new ObjectMapper(),
              Optional.of(serviceAccountsService),
              Lists.emptyList(),
              Optional.empty(),
              fiatPermissionEvaluator))
            .setControllerAdvice(
              new GenericExceptionHandlers(
                new ExceptionMessageDecorator(Mock(ObjectProvider) as ObjectProvider<List<ExceptionMessage>>)
              )
            )
            .build()
          mockMvcTest.perform(get("/pipelines/test"))
        } as Callable))
      }
      futures.each {results.add(it.get())}
    } finally {
      threadPool.shutdown()
    }

    then:
    results.each {
      it.andExpect(jsonPath('$.[*].name').value(["a1", "b1", "a3", "b", "c"]))
        .andExpect(jsonPath('$.[*].index').value([0, 1, 2, 3, 4]))
    }
  }

  def "multi-threaded cache refresh with synchronization"() {
    given:
    pipelineDAOConfigProperties.setSynchronizeCacheRefresh(true)

    pipelineDAO.create(null, new Pipeline([
      name: "c", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a3", application: "test", index: 3
    ]))

    when:
    def results = new ArrayList(10)
    def futures = new ArrayList(10)

    def threadPool = Executors.newFixedThreadPool(10)
    try {
      10.times {
        futures.add(threadPool.submit({ ->
          def mockMvcSynchronizationTest = MockMvcBuilders
            .standaloneSetup(new PipelineController(pipelineDAO,
              new ObjectMapper(),
              Optional.of(serviceAccountsService),
              Lists.emptyList(),
              Optional.empty(),
              fiatPermissionEvaluator))
            .setControllerAdvice(
              new GenericExceptionHandlers(
                new ExceptionMessageDecorator(Mock(ObjectProvider) as ObjectProvider<List<ExceptionMessage>>)
              )
            )
            .build()
          mockMvcSynchronizationTest.perform(get("/pipelines/test"))
        } as Callable))
      }
      futures.each {results.add(it.get())}
    } finally {
      threadPool.shutdown()
    }

    then:
    results.each {
      it.andExpect(jsonPath('$.[*].name').value(["a1", "b1", "a3", "b", "c"]))
        .andExpect(jsonPath('$.[*].index').value([0, 1, 2, 3, 4]))
    }
  }

  def "multi-threaded cache refresh with no synchronization and multiple read/writes"() {
    given:
    pipelineDAO.create(null, new Pipeline([
      name: "c", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a3", application: "test", index: 3
    ]))

    when:
    def results = new ArrayList(10)
    def futures = new ArrayList(10)

    def threadPool = Executors.newFixedThreadPool(10)
    try {
      10.times {
        futures.add(threadPool.submit({ ->
          def mockMvcTest = MockMvcBuilders
            .standaloneSetup(new PipelineController(pipelineDAO,
              new ObjectMapper(),
              Optional.of(serviceAccountsService),
              Lists.emptyList(),
              Optional.empty(),
              fiatPermissionEvaluator))
            .setControllerAdvice(
              new GenericExceptionHandlers(
                new ExceptionMessageDecorator(Mock(ObjectProvider) as ObjectProvider<List<ExceptionMessage>>)
              )
            )
            .build()

          if (it % 2 == 0) {
            mockMvcTest.perform(post('/pipelines')
              .contentType(MediaType.APPLICATION_JSON)
              .content(new ObjectMapper().writeValueAsString([
                name: "My Pipeline" + it,
                application: "test" + it,
                id: "id" + it,
                triggers: []])))
              .andReturn()
              .response
          }

          mockMvcTest.perform(get("/pipelines/test"))
        } as Callable))
      }
      futures.each {results.add(it.get())}
    } finally {
      threadPool.shutdown()
    }

    then:
    results.each {
      it.andExpect(jsonPath('$.[*].name').value(["a1", "b1", "a3", "b", "c"]))
        .andExpect(jsonPath('$.[*].index').value([0, 1, 2, 3, 4]))
    }
  }

  def "multi-threaded cache refresh with synchronization and multiple read/writes"() {
    given:
    pipelineDAOConfigProperties.setSynchronizeCacheRefresh(true)

    pipelineDAO.create(null, new Pipeline([
      name: "c", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b", application: "test"
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "b1", application: "test", index: 1
    ]))
    pipelineDAO.create(null, new Pipeline([
      name: "a3", application: "test", index: 3
    ]))

    when:
    def results = new ArrayList(10)
    def futures = new ArrayList(10)
    def threadPool = Executors.newFixedThreadPool(10)
    try {
      10.times {
        futures.add(threadPool.submit({ ->
          def mockMvcSynchronizationTest = MockMvcBuilders
            .standaloneSetup(new PipelineController(pipelineDAO,
              new ObjectMapper(),
              Optional.of(serviceAccountsService),
              Lists.emptyList(),
              Optional.empty(),
              fiatPermissionEvaluator))
            .setControllerAdvice(
              new GenericExceptionHandlers(
                new ExceptionMessageDecorator(Mock(ObjectProvider) as ObjectProvider<List<ExceptionMessage>>)
              )
            )
            .build()

          if (it % 2 == 0) {
            mockMvcSynchronizationTest.perform(post('/pipelines')
              .contentType(MediaType.APPLICATION_JSON)
              .content(new ObjectMapper().writeValueAsString([
                name: "My Pipeline" + it,
                application: "test" + it,
                id: "id" + it,
                triggers: []]
              )))
              .andReturn()
              .response
          }
          mockMvcSynchronizationTest.perform(get("/pipelines/test"))
        } as Callable))
      }
      futures.each {results.add(it.get())}
    } finally {
      threadPool.shutdown()
    }

    then:
    results.each {
      it.andExpect(jsonPath('$.[*].name').value(["a1", "b1", "a3", "b", "c"]))
        .andExpect(jsonPath('$.[*].index').value([0, 1, 2, 3, 4]))
    }
  }
}

@IgnoreIf({ S3TestHelper.s3ProxyUnavailable() })
class S3PipelineControllerTck extends PipelineControllerTck {
  @Shared
  def scheduler = Schedulers.from(Executors.newFixedThreadPool(1))

  @Shared
  PipelineDAO pipelineDAO

  @Override
  PipelineDAO createPipelineDAO() {
    def amazonS3 = new AmazonS3Client(new ClientConfiguration())
    amazonS3.setEndpoint("http://127.0.0.1:9999")
    S3TestHelper.setupBucket(amazonS3, "front50")
    def storageService = new S3StorageService(new ObjectMapper(),
      amazonS3,
      "front50",
      "test",
      false,
      "us-east-1",
      true,
      10_000,
      null)

    pipelineDAOConfigProperties.setRefreshMs(0)
    pipelineDAOConfigProperties.setShouldWarmCache(false)

    pipelineDAO = new DefaultPipelineDAO(storageService,
      scheduler,
      new DefaultObjectKeyLoader(storageService),
      pipelineDAOConfigProperties,
      new NoopRegistry(),
      CircuitBreakerRegistry.ofDefaults())

    return pipelineDAO
  }
}

class SqlPipelineControllerTck extends PipelineControllerTck {
  @Shared
  def scheduler = Schedulers.from(Executors.newFixedThreadPool(1))

  @AutoCleanup("close")
  SqlTestUtil.TestDatabase database = SqlTestUtil.initTcMysqlDatabase()

  SqlStorageService storageService

  def cleanup() {
    SqlTestUtil.cleanupDb(database.context)
  }

  @Override
  PipelineDAO createPipelineDAO() {
    storageService = new SqlStorageService(
      new ObjectMapper(),
      new NoopRegistry(),
      database.context,
      Clock.systemDefaultZone(),
      new SqlRetryProperties(),
      1,
      "default"
    )

    pipelineDAOConfigProperties.setRefreshMs(0)
    pipelineDAOConfigProperties.setShouldWarmCache(false)
    pipelineDAO = new DefaultPipelineDAO(
      storageService,
      scheduler,
      new DefaultObjectKeyLoader(storageService),
      pipelineDAOConfigProperties,
      new NoopRegistry(),
      CircuitBreakerRegistry.ofDefaults())

    // refreshing to initialize the cache with empty set
    pipelineDAO.all(true)

    return pipelineDAO
  }

  @Ignore("Re-enable when bulk saves are implemented")
  def "should optimally refresh the cache after updates and deletes"() {
    given:
    pipelineDAOConfigProperties.setOptimizeCacheRefreshes(true)
    def pipelines = [
      new Pipeline([name: "Pipeline1", application: "test", id: "id1", triggers: []]),
      new Pipeline([name: "Pipeline2", application: "test", id: "id2", triggers: []]),
      new Pipeline([name: "Pipeline3", application: "test", id: "id3", triggers: []]),
      new Pipeline([name: "Pipeline4", application: "test", id: "id4", triggers: []]),
    ]
    pipelineDAO.bulkImport(pipelines)

    // Test cache refreshes for additions
    when:
    def response = mockMvc.perform(get('/pipelines/test'))

    then:
    response.andReturn().response.status == OK
    response.andExpect(jsonPath('$.[*].name')
      .value(["Pipeline1", "Pipeline2", "Pipeline3", "Pipeline4"]))

    // Test cache refreshes for updates
    when:
    // Update Pipeline 2
    mockMvc.perform(put('/pipelines/id2')
      .contentType(MediaType.APPLICATION_JSON)
      .content(new ObjectMapper().writeValueAsString(pipelines[1])))
      .andExpect(status().isOk())
    response = mockMvc.perform(get('/pipelines/test'))

    then:
    response.andReturn().response.status == OK
    // ordered of returned pipelines changes after update
    response.andExpect(jsonPath('$.[*].name')
      .value(["Pipeline1", "Pipeline3", "Pipeline4", "Pipeline2"]))

    // Test cache refreshes for deletes
    when:
    // Update 1 pipeline
    mockMvc.perform(delete('/pipelines/test/Pipeline1')).andExpect(status().isOk())
    response = mockMvc.perform(get('/pipelines/test'))

    then:
    response.andReturn().response.status == OK
    response.andExpect(jsonPath('$.[*].name')
      .value(["Pipeline3", "Pipeline4", "Pipeline2"]))
  }
}
