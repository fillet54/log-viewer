(ns eventlog.reagent.views.eventviewer.events
  (:require
   [cljs.reader :as reader]
   [eventlog.storage :as storage]
   [eventlog.reagent.views.eventviewer.events.base :as event-base]
   [eventlog.reagent.views.eventviewer.data :as data]
   [eventlog.reagent.views.eventviewer.db :as db]
   [re-frame.core :as rf]))

(rf/reg-fx
 :persist-layout
 (fn [layout]
   (storage/save-layout! db/layout-storage-key layout)))

(rf/reg-fx
 :notify-layout-resized
 (fn [_]
   (js/requestAnimationFrame
    (fn []
      (js/requestAnimationFrame
       (fn []
         (.dispatchEvent js/window (js/Event. "eventviewer:layout-resized"))))))))

(rf/reg-fx
 :load-core-event-log
 (fn [_]
   (-> (.fetch js/window "/api/core-event/log")
       (.then (fn [response]
                (if (.-ok response)
                  (.text response)
                  (throw (js/Error. (str "Failed to load log: " (.-status response)))))))
       (.then (fn [body]
                (rf/dispatch [:eventviewer/log-loaded (reader/read-string body)])))
       (.catch (fn [error]
                 (rf/dispatch [:eventviewer/log-load-failed (or (.-message error) "Failed to load log")]))))))

(rf/reg-event-fx
 :eventviewer/init
 (fn [_ _]
   {:db (assoc (assoc db/default-db :log (assoc db/default-log :status :loading))
               :layout (storage/load-layout db/layout-storage-key))
    :load-core-event-log true}))

(rf/reg-event-fx
 :eventviewer/update-layout
 (fn [{:keys [db]} [_ f & args]]
   (let [layout (apply f (:layout db) args)]
     {:db (assoc db :layout layout)
      :persist-layout layout
      :notify-layout-resized true})))

(rf/reg-event-fx
 :eventviewer/reset-layout
 (fn [{:keys [db]} _]
   {:db (assoc db :layout storage/default-layout)
    :persist-layout storage/default-layout
    :notify-layout-resized true}))

(rf/reg-event-db
 :eventviewer/log-loaded
 (fn [db [_ events]]
   (let [events (->> events
                     (map-indexed (fn [index event]
                                    (assoc event :row-id index)))
                     vec)
         first-event (first events)
         first-time (some-> first-event event-base/event-time-ms)]
     (assoc db :log {:status :ready
                     :events events
                     :error nil
                     :selected-row-id (:row-id first-event)
                     :viewport-time (when-not (js/isNaN first-time) first-time)
                     :expanded-paths (if first-event
                                       (data/expanded-paths-for first-event)
                                       #{})}))))

(rf/reg-event-db
 :eventviewer/log-load-failed
 (fn [db [_ error]]
   (assoc db :log {:status :error
                   :events []
                   :error error
                   :selected-row-id nil
                   :viewport-time nil
                   :expanded-paths #{}})))

(rf/reg-event-db
 :eventviewer/select-event
 (fn [db [_ row-id]]
   (let [event (some #(when (= (:row-id %) row-id) %) (get-in db [:log :events]))]
     (-> db
         (assoc-in [:log :selected-row-id] row-id)
         (assoc-in [:log :expanded-paths] (if event
                                            (data/expanded-paths-for event)
                                            #{}))))))

(rf/reg-event-db
 :eventviewer/toggle-path
 (fn [db [_ path]]
   (update-in db [:log :expanded-paths]
              (fn [expanded]
                (if (contains? expanded path)
                  (disj expanded path)
                  (conj expanded path))))))

(rf/reg-event-db
 :eventviewer/expand-all
 (fn [db _]
   (if-let [event (db/selected-event-from-db db)]
     (assoc-in db [:log :expanded-paths] (data/expanded-paths-for event))
     db)))

(rf/reg-event-db
 :eventviewer/collapse-all
 (fn [db _]
   (assoc-in db [:log :expanded-paths] #{})))

(rf/reg-event-db
 :eventviewer/set-viewport-time
 (fn [db [_ viewport-time]]
   (assoc-in db [:log :viewport-time] viewport-time)))
