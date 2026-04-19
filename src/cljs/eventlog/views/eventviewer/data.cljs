(ns eventlog.views.eventviewer.data
  (:require
   [cljs.reader :as reader]
   [reagent.core :as r]))

(defn make-log-state []
  (r/atom {:status :idle
           :events []
           :error nil}))

(defn load-core-event-log! [log-state]
  (swap! log-state assoc :status :loading :error nil)
  (-> (.fetch js/window "/api/core-event/log")
      (.then (fn [response]
               (if (.-ok response)
                 (.text response)
                 (throw (js/Error. (str "Failed to load log: " (.-status response)))))))
      (.then (fn [body]
               (let [events (reader/read-string body)]
                 (swap! log-state assoc :status :ready :events events :error nil))))
      (.catch (fn [error]
                (swap! log-state assoc
                       :status :error
                       :events []
                       :error (or (.-message error) "Failed to load log"))))))
