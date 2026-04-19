(ns eventlog.views.eventviewer.events.row
  (:require
   [eventlog.views.eventviewer.events.base :as base]
   [eventlog.views.eventviewer.events.core-event :as core-event]))

(defn unknown-row [event _index]
  [:article.log-line
   [:span.log-details
    [:span.log-title-row
     [:span.log-name (or (:name event) "Unknown Event")]
     [:span.log-pill.log-kind (or (base/event-type event) "unknown")]]
    [:span.log-desc "No renderer registered for this event type."]]])

(defn render-row [event index]
  (case (base/event-type event)
    "core-event" [core-event/log-row event index]
    [unknown-row event index]))
