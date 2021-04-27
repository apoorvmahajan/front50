package com.netflix.spinnaker.front50;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.fiat.shared.FiatService;
import com.netflix.spinnaker.front50.model.StorageService;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.util.UriComponentsBuilder;

/** Test the behavior of the "real" web server in front50 -- tomcat in this case. */
// There are some choices about what to mock.  In general, the beans we need to
// provide are the ones Front50WebConfig and CommonStorageDAOConfig ask for.  We
// can do that by providing a @MockBean StorageService and a special mock
// ApplicationDAO bean that has a health interval greater than 0.  Or a
// similarly special mock StorageService bean.  Seems simpler to provide the one
// special mock alone, than with the additional @MockBean, even though this
// depends on some implementation details..
//
// Using @MockBean to create a StorageService mock, and then setting its health
// interval during setup() is too late for the rest of spring's autowiring to
// succeed.

// Alternatively, we can provide @MockBeans for all the DAO objects from
// CommonStorageDAOConfig, and all the health indicators from Front50WebConfig.
// It's a more delicate mocking operation to make a special mock (i.e. not using
// @MockBean), but it feels less brittle.  For example, if the implementation
// changes to use more *DAO objects, this test doesn't need to change.
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {WebEnvironmentTest.MockHelper.class, Main.class})
@TestPropertySource(
    properties = {
      "spring.application.name = front50",
      "logging.level.org.apache.coyote.http11.Http11InputBuffer = DEBUG",
    })
class WebEnvironmentTest {

  /** FiatAuthenticationConfig's requires a FiatService bean, so provide one. */
  @MockBean FiatService fiatService;

  @LocalServerPort int port;

  @Autowired TestRestTemplate restTemplate;

  @Test
  void testTomcatWithIllegalHttpHeaders() throws Exception {
    HttpHeaders headers = new HttpHeaders();
    // this is enough to cause a BAD_REQUEST if tomcat has rejectIllegalHeaders
    // set to true
    headers.add("X-Dum@my", "foo");

    // GET /pipelines is an arbitrary endpoint
    URI uri =
        UriComponentsBuilder.fromHttpUrl("http://localhost/pipelines")
            .port(port)
            .queryParam("restricted", false)
            .build()
            .toUri();

    ResponseEntity<String> entity =
        restTemplate.exchange(uri, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    assertEquals(HttpStatus.BAD_REQUEST, entity.getStatusCode());
  }

  public static class MockHelper {
    @Bean
    StorageService mockStorageServiceBean() {
      StorageService storageService = Mockito.mock(StorageService.class);
      // Without a valid health interval, the constructor for the
      // applicationDAOHealthIndicator bean (class ItemDAOHealthIndicator) from
      // Front50WebConfig fails.  The delay passed to scheduleWithFixedDelay
      // must be greater than 0.  The ItemDAO argument to the
      // ItemDAOHealthIndicator constructor in this case is a
      // DefaultApplicationDAO object -- the applicationDAO bean from
      // CommonStorageServiceDAOConfig.  DefaultApplicationDAO implements
      // ApplicationDAO, which extends ItemDAO, and ItemDAO has a default
      // implementation of getHealthIntervalMillis that returns 30 milliseconds.
      // However, DefaultApplicationDAO also extends StorageServiceSupport
      // that has its own getHealthIntervalMillis method and that's the one used
      // in this case.  StorageServiceSupport's getHealthIntervalMillis method
      // uses the value from the underlying StorageService object, which in this
      // test is our mock, so provide a valid value.
      when(storageService.getHealthIntervalMillis()).thenReturn(99L); // arbitrary value > 0
      return storageService;
    }
  }
}
