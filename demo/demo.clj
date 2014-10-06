(ns demo
  (:require [port-kafka.core :as port-kafka]
            [port-kafka.consumer :as c]
            [port-kafka.messages :as m]
            [port-kafka.producer :as p]))


(def producer-config {"metadata.broker.list" "localhost:9092"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

(def consumer-config {"zookeeper.connect" "localhost:2181"
                      "group.id" "port-kafka.demo"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "false"})

(defn create-bird []
  {:bird (rand-int 10000)})

(defn create-dog []
  {:dog (rand-int 10000)})

(defn create-cat []
  {:cat (rand-int 10000)})

(defn create-animal-messages [producer]
  (doseq [bird (map (fn [b] (m/create-message "birds" b :json))
                    (take 100 (repeatedly create-bird)))]
    (p/send! producer bird))
  (doseq [dog (map (fn [d] (m/create-message "dogs" d :json))
                   (take 100 (repeatedly create-dog)))]
    (p/send! producer dog))
  (doseq [ca (map (fn [cat] (m/create-message "cats" cat :json))
                  (take 100 (repeatedly create-cat)))]
    (p/send! producer ca)))

(defn cat-handler [message consumer]
  (println "Got a cat! " message))

(defn dog-handler [message consumer]
  (println "Got a dog! " message))

(defn bird-handler [message consumer]
  (println "Got a bird! " message))

(defn run-demo []
  (let [producer (p/create producer-config)
        _ (create-animal-messages producer)
        consumer (c/create consumer-config)
        topic-config {"birds" (int 1)
                      "cats" (int 1)
                      "dogs" (int 1)}
        consumer-topic-streams (c/create-topic-streams consumer topic-config)
        topic-handlers {:birds {:handler bird-handler
                                :buffer {:buffer-type :blocking
                                         :buffer-size 10}}
                        :cats {:handler cat-handler
                               :buffer {:buffer-type :blocking
                                        :buffer-size 10}}
                        :dogs {:handler dog-handler
                               :buffer {:buffer-type :blocking
                                        :buffer-size 10}}}]
    (port-kafka/consume! consumer consumer-topic-streams topic-handlers :json)))
