(ns eventlog.core-event-log
  (:require
   [com.stuartsierra.component :as component]
   [eventlog.sample.core-event :as sample]))

(defn generate-log []
  (let [event-list (sample/load-edn-resource "data/core_event/core_event_list.edn")
        subsystem-map (sample/load-edn-resource "data/core_event/subsystem.edn")]
    (sample/generate-events {:event-list event-list
                             :subsystem-map subsystem-map
                             :channels 4
                             :hours 12})))

(defrecord CoreEventLogStore [events]
  component/Lifecycle
  (start [this]
    (if events
      this
      (assoc this :events (generate-log))))
  (stop [this]
    (assoc this :events nil)))

(defn new-core-event-log-store []
  (map->CoreEventLogStore {}))
