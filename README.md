metrics-opentsdb
================

Compatible with metrics 3.0.1

Extension to Codahale metrics for using with OpenTSDB. 

Sends through a local udp_bridge tcollector process by default.
(https://github.com/OpenTSDB/tcollector/blob/master/collectors/0/udp_bridge.py)

    final MetricRegistry registry = new MetricRegistry();
    final OpenTSDB opentsdb = new OpenTSDB();
    final OpenTSDBReporter reporter = OpenTSDBReporter.forRegistry(registry)
                                                      .prefixedWith("java")
                                                      .convertRatesTo(TimeUnit.SECONDS)
                                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                      .filter(MetricFilter.ALL)
                                                      .build(opentsdb);

Then use metrics as normal.
