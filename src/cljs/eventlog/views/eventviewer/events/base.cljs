(ns eventlog.views.eventviewer.events.base)

(defprotocol EventRow
  (event-type [event])
  (event-time [event]))

(extend-protocol EventRow
  cljs.core/PersistentArrayMap
  (event-type [event]
    (:type event))
  (event-time [event]
    (:time event))

  cljs.core/PersistentHashMap
  (event-type [event]
    (:type event))
  (event-time [event]
    (:time event)))
