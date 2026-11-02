/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.hikaricp.v3_0;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

class HikariMaintenanceContextTest {

  @RegisterExtension
  static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @Test
  @Timeout(90)
  void lifetimeReplacementsMustNotInheritTheInitialBorrowersContext() throws Exception {
    List<SpanContext> creationContexts = new CopyOnWriteArrayList<>();
    CountDownLatch threeCreations = new CountDownLatch(3);
    DataSource source = mock(DataSource.class);
    when(source.getConnection())
        .thenAnswer(
            ignored -> {
              Connection connection = mock(Connection.class);
              when(connection.isValid(anyInt())).thenReturn(true);
              when(connection.getAutoCommit()).thenReturn(true);
              when(connection.getTransactionIsolation())
                  .thenReturn(Connection.TRANSACTION_READ_COMMITTED);
              creationContexts.add(Span.current().getSpanContext());
              threeCreations.countDown();
              return connection;
            });

    HikariConfig config = new HikariConfig();
    config.setDataSource(source);
    config.setMinimumIdle(0);
    config.setMaximumPoolSize(1);
    config.setInitializationFailTimeout(-1);
    // Exercise Hikari's real lifetime scheduling, including its minimum accepted lifetime.
    config.setMaxLifetime(30_000);
    config.setConnectionTimeout(10_000);
    AtomicReference<SpanContext> requestContext = new AtomicReference<>();

    // Initialize the pool outside the request; only the borrow should capture request context.
    try (HikariDataSource pool = new HikariDataSource(config)) {
      testing.runWithSpan(
          "initial-borrow",
          () -> {
            requestContext.set(Span.current().getSpanContext());
            try (Connection ignored = pool.getConnection()) {
              pool.setMinimumIdle(1);
            }
          });

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(threeCreations.await(75, SECONDS)).as("two native lifetime replacements").isTrue();
      assertThat(requestContext.get().isValid()).isTrue();
      assertThat(creationContexts.get(0))
          .as("request-triggered initial creation")
          .isEqualTo(requestContext.get());
      assertThat(creationContexts.subList(1, 3))
          .as("request-independent maintenance generations")
          .allSatisfy(context -> assertThat(context.isValid()).isFalse());
    }
  }

  @Test
  void ordinaryApplicationTasksKeepCapturedContextAfterTheParentEnds() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch releaseTask = new CountDownLatch(1);
    AtomicReference<SpanContext> requestContext = new AtomicReference<>();
    try {
      Future<SpanContext> task =
          testing.runWithSpan(
              "application-request",
              () -> {
                requestContext.set(Span.current().getSpanContext());
                return executor.submit(
                    () -> {
                      if (!releaseTask.await(10, SECONDS)) {
                        throw new AssertionError("application task was not released");
                      }
                      return Span.current().getSpanContext();
                    });
              });
      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      releaseTask.countDown();
      assertThat(requestContext.get().isValid()).isTrue();
      assertThat(task.get(10, SECONDS)).isEqualTo(requestContext.get());
    } finally {
      releaseTask.countDown();
      executor.shutdownNow();
      assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
    }
  }
}
