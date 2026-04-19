(ns eventlog.views.eventviewer.system
  (:require
   [eventlog.views.eventviewer.drag :as drag]
   [eventlog.views.eventviewer.events]
   [eventlog.views.eventviewer.subs]
   [re-frame.core :as rf]
   [reagent.core :as r]))

(defrecord AppSystem [drag-controller])

(defn make-system []
  (let [drag-controller (drag/make-drag-controller)]
    (->AppSystem drag-controller)))

(defonce system* (r/atom nil))

(defn start-system! []
  (let [system (make-system)]
    (drag/install! (:drag-controller system))
    (rf/dispatch-sync [:eventviewer/init])
    (reset! system* system)
    system))

(defn stop-system! []
  (when-let [system @system*]
    (drag/uninstall! (:drag-controller system))
    (reset! system* nil)))
