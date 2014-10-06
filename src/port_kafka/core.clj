(ns port-kafka.core
  (:require [clojure.core.async :refer [pub sub chan thread <!! >!! sliding-buffer dropping-buffer buffer]]
            [port-kafka.consumer :as c]
            [port-kafka.messages :as m])
  (:import [kafka.consumer KafkaStream]
           [kafka.message MessageAndMetadata]))

(defn- consume-topic-streams
  "Consumes each message stream for one topic on an async thread"
  [streams prod-port format]
  (doseq [^KafkaStream stream streams]
    (thread (doseq [^MessageAndMetadata message stream]
              (>!! prod-port (m/message->map message format))))))

(defn- consume-topics
  "Consumes all the streams for each topic"
  [keyed-streams topics prod-port format]
  (doseq [topic topics]
    (consume-topic-streams (c/get-streams-for-topic keyed-streams topic)
                           prod-port
                           format)))

(defn- subscribe-port
  "Subscribes a channel to its topic on the pub channel"
  [pub-port topic sub-port]
  (sub pub-port topic sub-port))

(def buffer-type-map
  {:sliding sliding-buffer
   :dropping dropping-buffer
   :blocking buffer})

(defn buffer-types []
  "A public function that describes the type of buffers supported for a topic channel."
  (keys buffer-type-map))

(defn- create-topic-buffer
  "Creates the buffer for a topic given a type and a size."
  [buffer-type buffer-size]
  ((buffer-type buffer-type-map) buffer-size))

(defn- create-buffer-fn
  "Given the topic configuration map, creates the buffer-fn for the pub channel."
  [topic-handlers]
  (let [topic-to-buffer-map (reduce-kv (fn [a k v]
                                         (assoc a k (create-topic-buffer (get-in v [:buffer :buffer-type])
                                                                         (get-in v [:buffer :buffer-size]))))
                                       {} topic-handlers)]
    (fn [topic]
      (topic topic-to-buffer-map))))

(defn- wrap-handler
  "Wraps a callback handler for a topic in a recurring fn of no args, taking a message off the channel
   and calling the handler with that message and the consumer as args."
  [sub-port handler consumer]
  (fn []
    (when-let [m (<!! sub-port)]
      (handler m consumer)
      (recur))))

(defn- create-wrapped-handlers
  "Creates and subscribes a channel for each topic to the pub channel, then wraps that topics handler.
   Returns a collection of wrapped functions."
  [pub-port topic-handlers consumer]
  (reduce-kv (fn [a k v]
               (let [sp (chan 1)]
                 (subscribe-port pub-port k sp)
                 (conj a (wrap-handler sp (:handler v) consumer))))
             [] topic-handlers))

(defn- exec-handlers
  "Executes each wrapped handler fn in an async thread."
  [wrapped-handlers]
  (doseq [wrapped-handler wrapped-handlers]
    (thread (wrapped-handler))))

(defn consume!
  "Consumes the streams for a consumer, creating a thread for each message stream for a topic and a thread for each message handler for a topic.
   Takes the consumer, the streams for it, a map of {:topic1 {:handler fn
                                                             :buffer {:buffer-type :keyword
                                                                      :buffer-size long}}
                                                    :topic2 ...}
   where :handler is a fn of two args: [message consumer] and :buffer-type is one of :sliding :dropping or :blocking
   and :buffer-size is the size of that buffer type, and a transit encoding format of :json :json-verbose or :msgpack Returns nil."
  [consumer keyed-consumer-streams topic-handlers format]
  (let [all-topics (keys topic-handlers)
        prod-port (chan 1)
        pub-port (pub prod-port :topic (create-buffer-fn topic-handlers))
        wrapped-handlers (create-wrapped-handlers pub-port topic-handlers consumer)]
    (consume-topics keyed-consumer-streams (map name all-topics) prod-port format)
    (exec-handlers wrapped-handlers)))
