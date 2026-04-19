(ns eventlog.app
  (:require
   [eventlog.views.eventviewer.root :as root]))

(defn ^:export init []
  (root/init))
