(defproject org.clojars.wgb/port-kafka "0.1.0"
  :description "Asynchronous Clojure Library for Apache Kafka"
  :url "http://github.com/wgb/port-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.cognitect/transit-clj "0.8.259"]
                 [org.apache.kafka/kafka_2.10 "0.8.1.1"]]
  :exclusions [javax.mail/mail
               javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri
               jline/jline])
