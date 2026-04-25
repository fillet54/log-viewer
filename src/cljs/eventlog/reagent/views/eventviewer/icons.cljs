(ns eventlog.reagent.views.eventviewer.icons)

(defn chevron-icon [direction]
  [:svg.chevron-icon
   {:viewBox "0 0 16 16"
    :aria-hidden "true"}
   (case direction
     :left [:path {:d "M10 3 5 8l5 5"}]
     :right [:path {:d "m6 3 5 5-5 5"}]
     :up [:path {:d "m3 10 5-5 5 5"}]
     :down [:path {:d "M3 6l5 5 5-5"}]
     [:path {:d "m6 3 5 5-5 5"}])])
