# Release Notes

## 2017-12-05 Don't send Kafka Producer REQUEST count as a rate; send it as a straight count instead.
This requires upgrading haystack-metrics and haystack-logback-metrics-appender, and obtaining a different
type of Counter (a counter that resets to 0 each time getValue() is called). All count metrics in Kafka Producer
will no longer be rates.

## 2017-12-04 Change haystack-metrics-version from 0.2.6 to 0.2.7
This requires changing the name of the configuration pointing to the Graphite/InfluxDb
endpoint from `haystack.graphite.address` to `haystack.graphite.host`
