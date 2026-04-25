(ns eventlog.views.eventviewer.dummy-content)

(def dummy-stats
  [{:label "Rows" :value "12,480"}
   {:label "Warnings" :value "318"}
   {:label "Critical" :value "42"}])

(def sample-events
  [{:severity "Green" :label "Core link established" :meta "SYS / NAV / A"}
   {:severity "Yellow" :label "Guidance channel degraded" :meta "SYS / GNC / C"}
   {:severity "Red" :label "Thermal bound exceeded" :meta "SYS / TCS / B"}
   {:severity "Green" :label "Telemetry frame recovered" :meta "SYS / COM / D"}])

(defn stat-card [{:keys [label value]}]
  [:div.dummy-stat-card
   {:replicant/key label}
   [:div.dummy-stat-value value]
   [:div.dummy-stat-label label]])

(defn event-list []
  [:section.content-card.log-listing-panel
   [:div.dummy-layout-content
    [:div.dummy-section-title "Event Stream"]
    [:div.dummy-event-list
     (for [{:keys [severity label meta]} sample-events]
       [:div.dummy-event-row {:replicant/key label}
        [:span.dummy-severity {:data-severity severity} severity]
        [:span.dummy-event-label label]
        [:span.dummy-event-meta meta]])]]])

(defn chart [{:keys [compact?]}]
  [:section {:class (if compact?
                      "content-card is-compact"
                      "content-card")}
   [:div.chart-panel
    [:div.chart-panel-header
     [:div.chart-panel-title "Severity Histogram"]
     [:div.chart-panel-subtitle "Dummy chart surface for layout work"]]
    [:div.chart-panel-body
     [:div.dummy-chart-surface
      [:div.dummy-chart-bars
       (for [[idx height] (map-indexed vector [34 52 26 74 42 68 39 57 30 81 45 63])]
         [:span.dummy-chart-bar
          {:replicant/key idx
           :style {:height (str height "%")}}])]]]]])

(defn details []
  [:div.dummy-layout-content
   [:div.dummy-stat-grid
    (for [stat dummy-stats]
      (stat-card stat))]
   [:div.dummy-notes
    [:div.dummy-section-title "Body Layout"]
    [:p "This is placeholder content. The shell, side pane, bottom pane, splitters, collapse rails, and main-view switching are running through Replicant."]
    [:p "Drag the splitters to resize. Drag below the threshold to collapse a pane, or use the pane buttons and rails."]]])

(defn search []
  [:div.dummy-layout-content
   [:div.dummy-search-box "severity: red AND channel: A"]
   [:div.dummy-search-help "Search content is placeholder-only in this Replicant layout slice."]])

(def default-views
  {:list event-list
   :chart chart
   :details details
   :search search})

