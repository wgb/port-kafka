# port-kafka

An Asynchrous Clojure library for Apache Kafka

Uses [core.async](https://github.com/clojure/core.async) channels, pub/sub, and thread to handle messages for the topics that you want to consume from Kafka. Encodes/decodes message values using [Transit](https://github.com/cognitect/transit-clj).

[![Clojars Project](http://clojars.org/org.clojars.wgb/port-kafka/latest-version.svg)](http://clojars.org/org.clojars.wgb/port-kafka)

## Usage

The consumer, producer, and messages namespaces are, for the most part, thin wrappers around the Kafka java api.

#### Consume
```clojure
(ns some.ns
(:require [port-kafka.consumer :as c]
          [port-kafka.core :as pk]))

(defn consume-all-the-streams [...]
(let [consumer (c/create consumer-config)
      consumer-streams (c/create-topic-streams consumer {"my-topic" (int 1)
                                                         "other-topic" (int 1)})]
(pk/consume! consumer consumer-streams {:my-topic {:handler (fn [message csmr]
                                                              (println message))
                                                   :buffer {:buffer-type :blocking
                                                            :buffer-size 1}}
                                        :other-topic ...} :json)))
```

Each time a message is received for a topic, it will execute its handler fn. The handler fn must take two arguments; the message (a map) and the consumer. It will do this for each topic asynchronously.

Messages are a map of:
```clojure
{:topic keyword
 :key  string/nil
 :partition keyword
 :offset long
 :value anything}
```

## TODO
Better documentation is coming...

## License

Copyright © 2014 Worth Becker

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
