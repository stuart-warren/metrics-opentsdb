package com.stuartwarren.metrics.opentsdb;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a tcollector.
 *
 * @see <a href="http://opentsdb.net/tcollector.html">OpenTSDB - a distributed, scalable Time Series Database (TSDB)</a>
 */
public class OpenTSDBReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link OpenTSDBReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link OpenTSDBReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link OpenTSDBReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link OpenTSDBReporter} with the given properties, sending metrics using the
         * given {@link OpenTSDB} client.
         *
         * @param opentsdb a {@link OpenTSDB} client
         * @return a {@link OpenTSDBReporter}
         */
        public OpenTSDBReporter build(OpenTSDB opentsdb) {
            return new OpenTSDBReporter(registry,
                                        opentsdb,
                                        clock,
                                        prefix,
                                        rateUnit,
                                        durationUnit,
                                        filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDBReporter.class);

    private final OpenTSDB opentsdb;
    private final Clock clock;
    private final String prefix;

    private OpenTSDBReporter(MetricRegistry registry,
                             OpenTSDB opentsdb,
                             Clock clock,
                             String prefix,
                             TimeUnit rateUnit,
                             TimeUnit durationUnit,
                             MetricFilter filter) {
        super(registry, "opentsdb-reporter", filter, rateUnit, durationUnit);
        this.opentsdb = opentsdb;
        this.clock = clock;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        // oh it'd be lovely to use Java 7 here
        try {
            opentsdb.connect();

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                reportMetered(entry.getKey(), entry.getValue(), timestamp);
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue(), timestamp);
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to report to OpenTSDB", opentsdb, e);
        } finally {
            try {
                opentsdb.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from OpenTSDB", opentsdb, e);
            }
        }
    }

    private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        opentsdb.send(prefix(name, "max"), format(convertDuration(snapshot.getMax())), timestamp);
        opentsdb.send(prefix(name, "mean"), format(convertDuration(snapshot.getMean())), timestamp);
        opentsdb.send(prefix(name, "min"), format(convertDuration(snapshot.getMin())), timestamp);
        opentsdb.send(prefix(name, "stddev"),
                      format(convertDuration(snapshot.getStdDev())),
                      timestamp);
        opentsdb.send(prefix(name, "p50"),
                      format(convertDuration(snapshot.getMedian())),
                      timestamp);
        opentsdb.send(prefix(name, "p75"),
                      format(convertDuration(snapshot.get75thPercentile())),
                      timestamp);
        opentsdb.send(prefix(name, "p95"),
                      format(convertDuration(snapshot.get95thPercentile())),
                      timestamp);
        opentsdb.send(prefix(name, "p98"),
                      format(convertDuration(snapshot.get98thPercentile())),
                      timestamp);
        opentsdb.send(prefix(name, "p99"),
                      format(convertDuration(snapshot.get99thPercentile())),
                      timestamp);
        opentsdb.send(prefix(name, "p999"),
                      format(convertDuration(snapshot.get999thPercentile())),
                      timestamp);

        reportMetered(name, timer, timestamp);
    }

    private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
        opentsdb.send(prefix(name, "count"), format(meter.getCount()), timestamp);
        opentsdb.send(prefix(name, "m1_rate"),
                      format(convertRate(meter.getOneMinuteRate())),
                      timestamp);
        opentsdb.send(prefix(name, "m5_rate"),
                      format(convertRate(meter.getFiveMinuteRate())),
                      timestamp);
        opentsdb.send(prefix(name, "m15_rate"),
                      format(convertRate(meter.getFifteenMinuteRate())),
                      timestamp);
        opentsdb.send(prefix(name, "mean_rate"),
                      format(convertRate(meter.getMeanRate())),
                      timestamp);
    }

    private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        opentsdb.send(prefix(name, "count"), format(histogram.getCount()), timestamp);
        opentsdb.send(prefix(name, "max"), format(snapshot.getMax()), timestamp);
        opentsdb.send(prefix(name, "mean"), format(snapshot.getMean()), timestamp);
        opentsdb.send(prefix(name, "min"), format(snapshot.getMin()), timestamp);
        opentsdb.send(prefix(name, "stddev"), format(snapshot.getStdDev()), timestamp);
        opentsdb.send(prefix(name, "p50"), format(snapshot.getMedian()), timestamp);
        opentsdb.send(prefix(name, "p75"), format(snapshot.get75thPercentile()), timestamp);
        opentsdb.send(prefix(name, "p95"), format(snapshot.get95thPercentile()), timestamp);
        opentsdb.send(prefix(name, "p98"), format(snapshot.get98thPercentile()), timestamp);
        opentsdb.send(prefix(name, "p99"), format(snapshot.get99thPercentile()), timestamp);
        opentsdb.send(prefix(name, "p999"), format(snapshot.get999thPercentile()), timestamp);
    }

    private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
        opentsdb.send(prefix(name, "count"), format(counter.getCount()), timestamp);
    }

    private void reportGauge(String name, Gauge gauge, long timestamp) throws IOException {
        final String value = format(gauge.getValue());
        if (value != null) {
            opentsdb.send(prefix(name), value, timestamp);
        }
    }

    private String format(Object o) {
        if (o instanceof Float) {
            return format(((Float) o).doubleValue());
        } else if (o instanceof Double) {
            return format(((Double) o).doubleValue());
        } else if (o instanceof Byte) {
            return format(((Byte) o).longValue());
        } else if (o instanceof Short) {
            return format(((Short) o).longValue());
        } else if (o instanceof Integer) {
            return format(((Integer) o).longValue());
        } else if (o instanceof Long) {
            return format(((Long) o).longValue());
        }
        return null;
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    private String format(long n) {
        return Long.toString(n);
    }

    private String format(double v) {
        // the Carbon plaintext format is pretty underspecified, but it seems like it just wants
        // US-formatted digits
        return String.format(Locale.US, "%2.2f", v);
    }
}
