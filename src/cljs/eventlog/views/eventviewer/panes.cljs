(ns eventlog.views.eventviewer.panes
  (:require
   [eventlog.views.eventviewer.data :as data]
   [eventlog.views.eventviewer.charts.severity-histogram :as severity-histogram]
   [eventlog.views.eventviewer.drag :as drag]
   [eventlog.views.eventviewer.events.row :as event-row]
   [eventlog.views.eventviewer.icons :as icons]
   [eventlog.views.eventviewer.virtual-list :as virtual-list]
   [eventlog.views.eventviewer.toolbar :as toolbar]
   [re-frame.core :as rf]))

(declare split-view)

(declare data-tree-node)

(defn tree-label [value]
  (if (keyword? value)
    (name value)
    (str value)))

(defn data-tree-children [value]
  (cond
    (map? value) (seq value)
    (sequential? value) (map-indexed vector value)
    :else nil))

(defn data-tree-node [expanded-paths path key value]
  (let [children (data-tree-children value)
        branch? (boolean (seq children))
        expanded? (contains? expanded-paths path)]
    [:div.detail-tree-node
     [:div.detail-tree-row
      (if branch?
        [:button.detail-tree-toggle
         {:type "button"
          :on-click #(rf/dispatch [:eventviewer/toggle-path path])}
         (if expanded? "▾" "▸")]
        [:span.detail-tree-spacer])
      [:span.detail-tree-key (tree-label key)]
      (if branch?
        [:span.detail-tree-value.detail-tree-value-summary
         (cond
           (map? value) (str (count value) " keys")
           (sequential? value) (str (count value) " items")
           :else "")]
       [:span.detail-tree-value (pr-str value)])]
     (when (and branch? expanded?)
       [:div.detail-tree-children
        (for [[child-key child-value] children]
          ^{:key (str path "|" child-key)}
          [data-tree-node expanded-paths (conj path child-key) child-key child-value])])]))

(defn details-content [_]
  (let [{:keys [expanded-paths]} @(rf/subscribe [:eventviewer/log])]
    (if-let [selected-event @(rf/subscribe [:eventviewer/selected-event])]
    [:div.details-content
     [:div.details-summary
      [:h3.details-event-name (:name selected-event)]
      [:p.details-event-description (:description selected-event)]]
     [:div.details-section-header
      [:h4.details-section-title "Data"]
      [:div.panel-actions
       [:button.chrome-button.chrome-button-small
        {:type "button"
         :on-click #(rf/dispatch [:eventviewer/collapse-all])}
        "Collapse all"]
       [:button.chrome-button.chrome-button-small
        {:type "button"
         :on-click #(rf/dispatch [:eventviewer/expand-all])}
        "Expand all"]]]
     [:div.detail-tree
      [data-tree-node expanded-paths [] :data (:data selected-event)]]]
    [:div.placeholder-pane
     [:div.placeholder-label "Select an Event"]])))

(defn listing-view [{:keys [log-state]}]
  (let [{:keys [status events error selected-row-id]} log-state]
    [:section.content-card.log-listing-panel
     [:div.log-list
      (case status
        :loading [:div.placeholder-pane [:div.placeholder-label "Loading Core Event Log"]]
        :error [:div.placeholder-pane [:div.placeholder-label (or error "Failed to load log")]]
        :ready (if (seq events)
                 [virtual-list/virtual-list
                  {:rows events
                   :row-height 42
                   :overscan 10
                   :render-row event-row/render-row
                   :selected-row-id selected-row-id
                   :on-select #(rf/dispatch [:eventviewer/select-event (:row-id %)])}]
                 [:div.placeholder-pane [:div.placeholder-label "No Events"]])
        [:div.placeholder-pane [:div.placeholder-label "Loading Core Event Log"]])]]))

(defn chart-view [{:keys [compact? log-state]}]
  (let [{:keys [events status]} log-state]
    [:section.content-card {:class (when compact? "is-compact")}
     (if (and (= status :ready) (seq events))
       [:div.chart-panel
        [:div.chart-panel-header
         [:div.chart-panel-title (if compact? "Severity Histogram" "Severity Histogram")]
         [:div.chart-panel-subtitle "Stacked SET-event counts by severity across the timeline"]]
         [:div.chart-panel-body
         [severity-histogram/component {:events events
                                        :viewport-time (:viewport-time log-state)}]]]
       [:div.placeholder-pane
        [:div.placeholder-label (if compact? "Mini Chart" "Chart View")]])]))

(defn details-pane [_]
  [:aside.side-panel
   [toolbar/panel-header
    "Details"
    "Selection metadata"
    [toolbar/pane-toggle-button
     {:title "Collapse details"
      :icon :right
      :on-click #(rf/dispatch [:eventviewer/update-layout assoc :right-open? false])}]]
   [details-content nil]])

(defn search-pane [_]
  [:section.bottom-panel
   [toolbar/panel-header
    "Search"
    "Saved with local UI layout state"
    [toolbar/pane-toggle-button
     {:title "Collapse search"
      :icon :down
      :on-click #(rf/dispatch [:eventviewer/update-layout assoc :bottom-open? false])}]]
   [:div.placeholder-pane
    [:div.placeholder-label "Search"]]])

(defn collapsed-details-rail [_]
  [:button.collapsed-rail.collapsed-rail-vertical
   {:type "button"
    :title "Show details"
    :on-click #(rf/dispatch [:eventviewer/update-layout assoc :right-open? true])}
   [:span.collapsed-rail-icon [icons/chevron-icon :left]]
   [:span.collapsed-rail-text "Details"]])

(defn collapsed-search-rail [_]
  [:button.collapsed-rail.collapsed-rail-horizontal
   {:type "button"
    :title "Show search"
    :on-click #(rf/dispatch [:eventviewer/update-layout assoc :bottom-open? true])}
   [:span.collapsed-rail-icon [icons/chevron-icon :up]]
   [:span.collapsed-rail-text "Search"]])

(defn vertical-splitter [{:keys [drag-controller]}]
  [:div.splitter.splitter-vertical
   {:on-pointer-down #(drag/begin-drag! drag-controller :right %)}])

(defn horizontal-splitter [{:keys [drag-controller]}]
  [:div.splitter.splitter-horizontal
   {:on-pointer-down #(drag/begin-drag! drag-controller :bottom %)}])

(defn inner-splitter [{:keys [drag-controller]}]
  [:div.splitter.splitter-inner
   {:on-pointer-down #(drag/begin-drag! drag-controller :split-chart %)}])

(defn split-view [{:keys [layout drag-controller log-state]}]
  (let [{:keys [split-chart-size]} layout]
    [:section.split-stack
     {:id "split-stack"
      :style {:grid-template-rows (str split-chart-size "px 6px minmax(0, 1fr)")}}
     [chart-view {:compact? true :log-state log-state}]
     [inner-splitter {:drag-controller drag-controller}]
     [listing-view {:log-state log-state}]]))

(defn main-panel [{:keys [layout drag-controller main-view log-state]}]
  [:main.main-panel
   [toolbar/main-toolbar {:active-view main-view :log-state log-state}]
   [:div.main-body
    (case main-view
      :chart [chart-view {:compact? false :log-state log-state}]
      :split [split-view {:layout layout :drag-controller drag-controller :log-state log-state}]
      [listing-view {:log-state log-state}])]])
