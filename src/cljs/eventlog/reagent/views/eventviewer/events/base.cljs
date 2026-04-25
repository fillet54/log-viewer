(ns eventlog.reagent.views.eventviewer.events.base)

(defn primary-time-value [value]
  (if (vector? value)
    (first value)
    value))

(defn since-boot-value [value]
  (when (vector? value)
    (second value)))

(defprotocol EventRow
  (event-type [event])
  (event-time [event])
  (event-primary-time [event])
  (event-since-boot [event]))

(extend-protocol EventRow
  cljs.core/PersistentArrayMap
  (event-type [event]
    (:type event))
  (event-time [event]
    (:time event))
  (event-primary-time [event]
    (primary-time-value (:time event)))
  (event-since-boot [event]
    (since-boot-value (:time event)))

  cljs.core/PersistentHashMap
  (event-type [event]
    (:type event))
  (event-time [event]
    (:time event))
  (event-primary-time [event]
    (primary-time-value (:time event)))
  (event-since-boot [event]
    (since-boot-value (:time event))))

(defn event-time-ms [event]
  (let [value (js/Date.parse (event-primary-time event))]
    (when-not (js/isNaN value)
      value)))
