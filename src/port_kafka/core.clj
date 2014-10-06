(ns port-kafka.core
  (:require [clojure.core.async :refer [pub sub chan thread <!! >!! sliding-buffer dropping-buffer buffer]]
            [port-kafka.consumer :as c]
            [port-kafka.messages :as m])
  (:import [kafka.consumer KafkaStream]
           [kafka.message MessageAndMetadata]))

(defn- consume-topic-streams
  [streams pub-port format]
  (doseq [^KafkaStream stream streams]
    (thread (doseq [^MessageAndMetadata message stream]
              (>!! pub-port (m/message->map message format))))))

(defn- consume-topics
  [keyed-streams topics pub-port format]
  (doseq [topic topics]
    (consume-topic-streams (c/get-streams-for-topic keyed-streams topic)
                           pub-port
                           format)))

(defn- subscribe-port
  [prod-port topic sub-port]
  (sub prod-port topic sub-port))

(def buffer-type-map
  {:sliding sliding-buffer
   :dropping dropping-buffer
   :blocking buffer})

(defn buffer-types []
  (keys buffer-type-map))

(defn- create-topic-buffer
  [buffer-type buffer-size]
  ((buffer-type buffer-type-map) buffer-size))

(defn- create-buffer-fn
  [topic-handlers]
  (let [topic-to-buffer-map (reduce-kv (fn [a k v]
                                         (assoc a k (create-topic-buffer (get-in v [:buffer :buffer-type])
                                                                         (get-in v [:buffer :buffer-size]))))
                                       {} topic-handlers)]
    (fn [topic]
      (topic topic-to-buffer-map))))

(defn- wrap-handler
  [sub-port handler consumer]
  (fn []
    (when-let [m (<!! sub-port)]
      (handler m consumer)
      (recur))))

(defn- create-wrapped-handlers
  [prod-port topic-handlers consumer]
  (reduce-kv (fn [a k v]
               (let [sp (chan 1)]
                 (subscribe-port prod-port k sp)
                 (conj a (wrap-handler sp (:handler v) consumer))))
             [] topic-handlers))

(defn- exec-handlers
  [wrapped-handlers]
  (doseq [wrapped-handler wrapped-handlers]
    (thread (wrapped-handler))))

(defn consume!
  [consumer keyed-consumer-streams topic-handlers format]
  (let [all-topics (keys topic-handlers)
        pub-port (chan 1)
        prod-port (pub pub-port :topic (create-buffer-fn topic-handlers))
        wrapped-handlers (create-wrapped-handlers prod-port topic-handlers consumer)]
    (consume-topics keyed-consumer-streams (map name all-topics) pub-port format)
    (exec-handlers wrapped-handlers)))
