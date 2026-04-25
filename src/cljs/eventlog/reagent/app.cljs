(ns eventlog.reagent.app
  (:require
   [eventlog.reagent.views.eventviewer.root :as root]))

(defn ^:export init []
  (root/init))
