(ns eventlog.views.eventviewer.data
  (:require
   [cljs.reader :as reader]
   [reagent.core :as r]))

(defn branch-paths
  ([value]
   (branch-paths [] value))
  ([path value]
   (cond
     (map? value)
     (let [entries (seq value)]
       (into (if entries [path] [])
             (mapcat (fn [[key child]]
                       (branch-paths (conj path key) child)))
             entries))

     (sequential? value)
     (let [entries (seq value)]
       (into (if entries [path] [])
             (mapcat (fn [[idx child]]
                       (branch-paths (conj path idx) child)))
             (map-indexed vector value)))

     :else
     [])))

(defn make-log-state []
  (r/atom {:status :idle
           :events []
           :error nil
           :selected-row-id nil
           :expanded-paths #{}}))

(defn expanded-paths-for [event]
  (into #{}
        (branch-paths (:data event))))

(defn selected-event [log-state]
  (let [{:keys [events selected-row-id]} @log-state]
    (some #(when (= (:row-id %) selected-row-id) %) events)))

(defn select-event! [log-state row-id]
  (let [event (some #(when (= (:row-id %) row-id) %) (:events @log-state))]
    (swap! log-state assoc
           :selected-row-id row-id
           :expanded-paths (if event
                             (expanded-paths-for event)
                             #{}))))

(defn toggle-path! [log-state path]
  (swap! log-state update :expanded-paths
         (fn [expanded]
           (if (contains? expanded path)
             (disj expanded path)
             (conj expanded path)))))

(defn expand-all! [log-state]
  (when-let [event (selected-event log-state)]
    (swap! log-state assoc :expanded-paths (expanded-paths-for event))))

(defn collapse-all! [log-state]
  (swap! log-state assoc :expanded-paths #{}))

(defn load-core-event-log! [log-state]
  (swap! log-state assoc :status :loading :error nil)
  (-> (.fetch js/window "/api/core-event/log")
      (.then (fn [response]
               (if (.-ok response)
                 (.text response)
                 (throw (js/Error. (str "Failed to load log: " (.-status response)))))))
      (.then (fn [body]
               (let [events (->> (reader/read-string body)
                                 (map-indexed (fn [index event]
                                                (assoc event :row-id index)))
                                 vec)
                     first-event (first events)]
                 (swap! log-state assoc
                        :status :ready
                        :events events
                        :error nil
                        :selected-row-id (:row-id first-event)
                        :expanded-paths (if first-event
                                          (expanded-paths-for first-event)
                                          #{})))))
      (.catch (fn [error]
                (swap! log-state assoc
                       :status :error
                       :events []
                       :selected-row-id nil
                       :expanded-paths #{}
                       :error (or (.-message error) "Failed to load log"))))))
