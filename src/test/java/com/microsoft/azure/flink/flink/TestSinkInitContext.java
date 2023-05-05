package com.microsoft.azure.flink.flink;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

public class TestSinkInitContext implements Sink.InitContext {

  private static final TestProcessingTimeService processingTimeService;
  private final MetricListener metricListener = new MetricListener();
  private final OperatorIOMetricGroup operatorIOMetricGroup =
      UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
  private final SinkWriterMetricGroup metricGroup =
      InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup(), operatorIOMetricGroup);
  private final MailboxExecutor mailboxExecutor;

  StreamTaskActionExecutor streamTaskActionExecutor = new StreamTaskActionExecutor() {
    @Override
    public void run(RunnableWithException e) throws Exception {
      e.run();
    }

    @Override
    public <E extends Throwable> void runThrowing(ThrowingRunnable<E> throwingRunnable) throws E {
      throwingRunnable.run();
    }

    @Override
    public <R> R call(Callable<R> callable) throws Exception {
      return callable.call();
    }
  };

  public TestSinkInitContext() {
    mailboxExecutor = new MailboxExecutorImpl(new TaskMailboxImpl(Thread.currentThread()),
        Integer.MAX_VALUE, streamTaskActionExecutor);
  }

  static {
    processingTimeService = new TestProcessingTimeService();
  }

  @Override
  public UserCodeClassLoader getUserCodeClassLoader() {
    return null;
  }

  @Override
  public MailboxExecutor getMailboxExecutor() {
    return mailboxExecutor;
  }

  @Override
  public ProcessingTimeService getProcessingTimeService() {
    return new ProcessingTimeService() {
      @Override
      public long getCurrentProcessingTime() {
        return processingTimeService.getCurrentProcessingTime();
      }

      @Override
      public ScheduledFuture<?> registerTimer(long time,
          ProcessingTimeCallback processingTimerCallback) {
        processingTimeService.registerTimer(time, processingTimerCallback);
        return null;
      }
    };
  }

  @Override
  public int getSubtaskId() {
    return 0;
  }

  @Override
  public int getNumberOfParallelSubtasks() {
    return 0;
  }

  @Override
  public int getAttemptNumber() {
    return 0;
  }

  @Override
  public SinkWriterMetricGroup metricGroup() {
    return metricGroup;
  }

  @Override
  public OptionalLong getRestoredCheckpointId() {
    return OptionalLong.empty();
  }

  @Override
  public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
    return null;
  }

  public TestProcessingTimeService getTestProcessingTimeService() {
    return processingTimeService;
  }

  public Optional<Gauge<Long>> getCurrentSendTimeGauge() {
    return metricListener.getGauge("currentSendTime");
  }

  public Counter getNumRecordsOutCounter() {
    return metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
  }

  public Counter getNumBytesOutCounter() {
    return metricGroup.getIOMetricGroup().getNumBytesOutCounter();
  }
}