(ns port-kafka.util
  (:require [clojure.walk :refer [stringify-keys]])
  (:import [java.util Properties]))

(defn map->properties ^Properties
  [m]
  (doto (Properties.)
    (.putAll (stringify-keys m))))
