(ns port-kafka.consumer
  (:require [port-kafka.util :refer [map->properties]])
  (:import [kafka.consumer Consumer ConsumerConfig]
           [kafka.javaapi.consumer ConsumerConnector]))

(defn map->consumer-config ^ConsumerConfig
  [config]
  (ConsumerConfig. (map->properties config)))

(defn commit-offsets
  [^ConsumerConnector consumer & [retry?]]
  (if retry?
    (.commitOffsets consumer retry?)
    (.commitOffsets consumer)))

(defn get-streams-for-topic ^java.util.ArrayList
  [^java.util.Map stream-map topic]
  (.get stream-map topic))

(defn create-topic-streams ^java.util.Map
  [^ConsumerConnector consumer topic-config]
  (.createMessageStreams consumer topic-config))

(defn create ^ConsumerConnector
  [config]
  (Consumer/createJavaConsumerConnector (map->consumer-config config)))

(defn shutdown
  [^ConsumerConnector consumer]
  (.shutdown consumer))
