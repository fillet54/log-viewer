(ns eventlog.views.eventviewer.db
  (:require
   [eventlog.storage :as storage]))

(def layout-storage-key "eventlog.layout.v1")

(def default-log
  {:status :idle
   :events []
   :error nil
   :selected-row-id nil
   :expanded-paths #{}})

(def default-db
  {:layout storage/default-layout
   :log default-log})

(defn selected-event-from-db [db]
  (let [{:keys [events selected-row-id]} (:log db)]
    (some #(when (= (:row-id %) selected-row-id) %) events)))
