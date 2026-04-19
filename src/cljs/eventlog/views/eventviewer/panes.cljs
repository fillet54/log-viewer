(ns eventlog.views.eventviewer.panes
  (:require
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.data :as data]
   [eventlog.views.eventviewer.drag :as drag]
   [eventlog.views.eventviewer.events.row :as event-row]
   [eventlog.views.eventviewer.icons :as icons]
   [eventlog.views.eventviewer.virtual-list :as virtual-list]
   [eventlog.views.eventviewer.toolbar :as toolbar]))

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

(defn data-tree-node [log-state path key value]
  (let [children (data-tree-children value)
        branch? (boolean (seq children))
        expanded? (contains? (:expanded-paths @log-state) path)]
    [:div.detail-tree-node
     [:div.detail-tree-row
      (if branch?
        [:button.detail-tree-toggle
         {:type "button"
          :on-click #(data/toggle-path! log-state path)}
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
          [data-tree-node log-state (conj path child-key) child-key child-value])])]))

(defn details-content [{:keys [log-state]}]
  (if-let [selected-event (data/selected-event log-state)]
    [:div.details-content
     [:div.details-summary
      [:h3.details-event-name (:name selected-event)]
      [:p.details-event-description (:description selected-event)]]
     [:div.details-section-header
      [:h4.details-section-title "Data"]
      [:div.panel-actions
       [:button.chrome-button.chrome-button-small
        {:type "button"
         :on-click #(data/collapse-all! log-state)}
        "Collapse all"]
       [:button.chrome-button.chrome-button-small
        {:type "button"
         :on-click #(data/expand-all! log-state)}
        "Expand all"]]]
     [:div.detail-tree
      [data-tree-node log-state [] :data (:data selected-event)]]]
    [:div.placeholder-pane
     [:div.placeholder-label "Select an Event"]]))

(defn listing-view [{:keys [log-state]}]
  (let [{:keys [status events error selected-row-id]} @log-state]
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
                   :on-select #(data/select-event! log-state (:row-id %))}]
                 [:div.placeholder-pane [:div.placeholder-label "No Events"]])
        [:div.placeholder-pane [:div.placeholder-label "Loading Core Event Log"]])]]))

(defn chart-view [compact?]
  [:section.content-card {:class (when compact? "is-compact")}
   [:div.placeholder-pane
    [:div.placeholder-label (if compact? "Mini Chart" "Chart View")]]])

(defn details-pane [{:keys [layout-store log-state]}]
  [:aside.side-panel
   [toolbar/panel-header
    "Details"
    "Selection metadata"
    [toolbar/pane-toggle-button
     {:title "Collapse details"
      :icon :right
      :on-click #(storage/swap-layout! layout-store assoc :right-open? false)}]]
   [details-content {:log-state log-state}]])

(defn search-pane [{:keys [layout-store]}]
  [:section.bottom-panel
   [toolbar/panel-header
    "Search"
    "Saved with local UI layout state"
    [toolbar/pane-toggle-button
     {:title "Collapse search"
      :icon :down
      :on-click #(storage/swap-layout! layout-store assoc :bottom-open? false)}]]
   [:div.placeholder-pane
    [:div.placeholder-label "Search"]]])

(defn collapsed-details-rail [{:keys [layout-store]}]
  [:button.collapsed-rail.collapsed-rail-vertical
   {:type "button"
    :title "Show details"
    :on-click #(storage/swap-layout! layout-store assoc :right-open? true)}
   [:span.collapsed-rail-icon [icons/chevron-icon :left]]
   [:span.collapsed-rail-text "Details"]])

(defn collapsed-search-rail [{:keys [layout-store]}]
  [:button.collapsed-rail.collapsed-rail-horizontal
   {:type "button"
    :title "Show search"
    :on-click #(storage/swap-layout! layout-store assoc :bottom-open? true)}
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

(defn split-view [{:keys [layout-store drag-controller log-state]}]
  (let [{:keys [split-chart-size]} @(storage/layout-state layout-store)]
    [:section.split-stack
     {:id "split-stack"
      :style {:grid-template-rows (str split-chart-size "% 6px minmax(0, 1fr)")}}
     [chart-view true]
     [inner-splitter {:drag-controller drag-controller}]
     [listing-view {:log-state log-state}]]))

(defn main-panel [{:keys [layout-store drag-controller main-view log-state]}]
  [:main.main-panel
   [toolbar/main-toolbar {:layout-store layout-store :active-view main-view :log-state log-state}]
   [:div.main-body
    (case main-view
      :chart [chart-view false]
      :split [split-view {:layout-store layout-store :drag-controller drag-controller :log-state log-state}]
      [listing-view {:log-state log-state}])]])
