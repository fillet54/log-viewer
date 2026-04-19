(ns eventlog.system
  (:require
   [com.stuartsierra.component :as component]
   [eventlog.web :as web]))

(defn new-system [config]
  (component/system-map
   :web (web/new-web-server config)))
