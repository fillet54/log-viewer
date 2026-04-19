(ns eventlog.views.eventviewer.state)

(defn clamp [value min-value max-value]
  (-> value
      (max min-value)
      (min max-value)))

(defn set-main-view [layout view]
  (assoc layout :main-view view))
