(ns port-kafka.producer
  (:require [port-kafka.util :refer [map->properties]])
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]))

(defn map->producer-config ^ProducerConfig
  [config]
  (ProducerConfig. (map->properties config)))

(defn send!
  [^Producer producer ^KeyedMessage message]
  (.send producer message))

(defn create ^Producer
  [config]
  (Producer. (map->producer-config config)))

(defn close
  [^Producer producer]
  (.close producer))
