(ns port-kafka.messages
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [kafka.message MessageAndMetadata]
           [kafka.producer KeyedMessage]))

(defn- read-body
  "Reads the message body in the given format."
  [body format]
  (with-open [in-stream (ByteArrayInputStream. body)]
    (let [reader (transit/reader in-stream format)]
      (transit/read reader))))

(defn- write-body
  "Writes the message body in the given transit format."
  [body format]
  (with-open [out-stream (ByteArrayOutputStream.)]
    (let [writer (transit/writer out-stream format)]
      (transit/write writer body)
      (.toByteArray out-stream))))

(defn create-message
  "Creates a KeyedMessage instance whose value is a transit encoded ByteArray of format.
   format is a keyword of :json :json-verbose or :msgpack."
  ^KeyedMessage
  ([topic value format]
     (create-message topic value format nil))
  ([topic value format key]
     (KeyedMessage. topic key (write-body value format))))

(defn message->map
  "Takes a Kafka MessageAndMetadata instance whose message property is transit encoded and returns a clojure map
   whose :value is the decoded message. format is a keyword of :json :json-verbose or :msgpack."
  [^MessageAndMetadata message format]
  {:value (read-body (.message message) format)
   :topic (keyword (.topic message))
   :partition (keyword (.partition message))
   :key (.key message)
   :offset (.offset message)})
