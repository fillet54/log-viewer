(ns eventlog.views.eventviewer.icons)

(defn svg-attrs [attrs]
  (merge {:class "nav-icon"
          :viewBox "0 0 24 24"
          :fill "none"
          :stroke "currentColor"
          :stroke-width "2"
          :stroke-linecap "round"
          :stroke-linejoin "round"
          :aria-hidden "true"}
         attrs))

(defn icon-home
  ([] (icon-home {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "M3 11.5 12 4l9 7.5"}]
    [:path {:d "M5 10.5V20h5v-5h4v5h5v-9.5"}]]))

(defn icon-calendar
  ([] (icon-calendar {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:rect {:x "4" :y "5" :width "16" :height "15" :rx "2"}]
    [:path {:d "M8 3v4"}]
    [:path {:d "M16 3v4"}]
    [:path {:d "M4 10h16"}]]))

(defn icon-database
  ([] (icon-database {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:ellipse {:cx "12" :cy "5" :rx "7" :ry "3"}]
    [:path {:d "M5 5v6c0 1.7 3.1 3 7 3s7-1.3 7-3V5"}]
    [:path {:d "M5 11v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6"}]]))

(defn icon-activity
  ([] (icon-activity {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "M3 12h4l2.5-6 5 12 2.5-6h4"}]]))

(defn icon-user
  ([] (icon-user {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:circle {:cx "12" :cy "8" :r "4"}]
    [:path {:d "M4 21c1.5-4 4.2-6 8-6s6.5 2 8 6"}]]))

(defn icon-chevron-down
  ([] (icon-chevron-down {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "m6 9 6 6 6-6"}]]))

(defn icon-chevron-up
  ([] (icon-chevron-up {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "m6 15 6-6 6 6"}]]))

(defn icon-chevron-left
  ([] (icon-chevron-left {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "m15 6-6 6 6 6"}]]))

(defn icon-chevron-right
  ([] (icon-chevron-right {}))
  ([attrs]
   [:svg (svg-attrs attrs)
    [:path {:d "m9 6 6 6-6 6"}]]))

(def icons
  {:home icon-home
   :calendar icon-calendar
   :database icon-database
   :activity icon-activity
   :user icon-user
   :chevron-down icon-chevron-down
   :chevron-up icon-chevron-up
   :chevron-left icon-chevron-left
   :chevron-right icon-chevron-right})

(defn icon [name]
  (if-let [icon-fn (get icons name)]
    (icon-fn)
    [:span {:class "nav-icon nav-icon-fallback" :aria-hidden "true"} "•"]))

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

