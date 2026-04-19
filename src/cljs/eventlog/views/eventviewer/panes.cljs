(ns eventlog.views.eventviewer.panes
  (:require
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.drag :as drag]
   [eventlog.views.eventviewer.icons :as icons]
   [eventlog.views.eventviewer.toolbar :as toolbar]))

(declare split-view)

(defn listing-view []
  [:section.content-card
   [:div.placeholder-pane
    [:div.placeholder-label "Log Listing"]]])

(defn chart-view [compact?]
  [:section.content-card {:class (when compact? "is-compact")}
   [:div.placeholder-pane
    [:div.placeholder-label (if compact? "Mini Chart" "Chart View")]]])

(defn details-pane [{:keys [layout-store]}]
  [:aside.side-panel
   [toolbar/panel-header
    "Details"
    "Selection metadata"
    [toolbar/pane-toggle-button
     {:title "Collapse details"
      :icon :right
      :on-click #(storage/swap-layout! layout-store assoc :right-open? false)}]]
   [:div.placeholder-pane
    [:div.placeholder-label "Details"]]])

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

(defn split-view [{:keys [layout-store drag-controller]}]
  (let [{:keys [split-chart-size]} @(storage/layout-state layout-store)]
    [:section.split-stack
     {:id "split-stack"
      :style {:grid-template-rows (str split-chart-size "% 6px minmax(0, 1fr)")}}
     [chart-view true]
     [inner-splitter {:drag-controller drag-controller}]
     [listing-view]]))

(defn main-panel [{:keys [layout-store drag-controller main-view]}]
  [:main.main-panel
   [toolbar/main-toolbar {:layout-store layout-store :active-view main-view}]
   [:div.main-body
    (case main-view
      :chart [chart-view false]
      :split [split-view {:layout-store layout-store :drag-controller drag-controller}]
      [listing-view])]])
