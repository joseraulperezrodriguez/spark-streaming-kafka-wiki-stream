# Kafka+Spark Streaming, Wiki Demo

You can see the demo running live here: https://bigdata.stratebi.com/

<p>The project consists on two modules:</p>

- kafka
- spark

## kafka

This module contains the code for receive wiki data from stream using Server Sent Event and writing it to a Kafka cluster also read data from Kafka as a consumer to send a real time Summary to an API Rest.

## spark

This module contains the code that analyze in real time the stream received from Kafka
