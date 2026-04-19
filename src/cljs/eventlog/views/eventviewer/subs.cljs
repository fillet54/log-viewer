(ns eventlog.views.eventviewer.subs
  (:require
   [eventlog.views.eventviewer.db :as db]
   [re-frame.core :as rf]))

(rf/reg-sub
 :eventviewer/layout
 (fn [db _]
   (:layout db)))

(rf/reg-sub
 :eventviewer/log
 (fn [db _]
   (:log db)))

(rf/reg-sub
 :eventviewer/selected-event
 (fn [db _]
   (db/selected-event-from-db db)))
