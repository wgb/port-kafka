(ns port-kafka.messages
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [kafka.message MessageAndMetadata]
           [kafka.producer KeyedMessage]))

(defn- read-body
  [body format]
  (with-open [in-stream (ByteArrayInputStream. body)]
    (let [reader (transit/reader in-stream format)]
      (transit/read reader))))

(defn- write-body
  [body format]
  (with-open [out-stream (ByteArrayOutputStream.)]
    (let [writer (transit/writer out-stream format)]
      (transit/write writer body)
      (.toByteArray out-stream))))

(defn create-message ^KeyedMessage
  ([topic value format]
     (create-message topic value format nil))
  ([topic value format key]
     (KeyedMessage. topic key (write-body value format))))

(defn message->map
  [^MessageAndMetadata message format]
  {:value (read-body (.message message) format)
   :topic (keyword (.topic message))
   :partition (keyword (.partition message))
   :key (.key message)
   :offset (.offset message)})
