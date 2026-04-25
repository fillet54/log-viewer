(ns eventlog.views.eventviewer.icons)

(defn chevron-icon [direction]
  (let [points (case direction
                 :left "15 18 9 12 15 6"
                 :right "9 18 15 12 9 6"
                 :up "6 15 12 9 18 15"
                 :down "6 9 12 15 18 9"
                 "9 18 15 12 9 6")]
    [:svg.chevron-icon
     {:viewBox "0 0 24 24"
      :aria-hidden "true"
      :focusable "false"}
     [:polyline {:points points}]]))

