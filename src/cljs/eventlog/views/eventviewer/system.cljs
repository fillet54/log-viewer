(ns eventlog.views.eventviewer.system
  (:require
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.data :as data]
   [eventlog.views.eventviewer.drag :as drag]
   [reagent.core :as r]))

(defrecord AppSystem [layout-store drag-controller log-state])

(defn make-system []
  (let [layout-store (storage/make-layout-store "eventlog.layout.v1")
        drag-controller (drag/make-drag-controller layout-store)
        log-state (data/make-log-state)]
    (->AppSystem layout-store drag-controller log-state)))

(defonce system* (r/atom nil))

(defn start-system! []
  (let [system (make-system)]
    (storage/load-layout! (:layout-store system))
    (drag/install! (:drag-controller system))
    (data/load-core-event-log! (:log-state system))
    (reset! system* system)
    system))

(defn stop-system! []
  (when-let [system @system*]
    (drag/uninstall! (:drag-controller system))
    (reset! system* nil)))
