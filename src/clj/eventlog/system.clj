(ns eventlog.system
  (:require
   [com.stuartsierra.component :as component]
   [eventlog.core-event-log :as core-event-log]
   [eventlog.web :as web]))

(defn new-system [config]
  (component/system-map
   :core-event-log (core-event-log/new-core-event-log-store)
   :web (component/using
         (web/new-web-server config)
         [:core-event-log])))
