(ns eventlog.views.eventviewer.root
  (:require
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.panes :as panes]
   [eventlog.views.eventviewer.state :as state]
   [eventlog.views.eventviewer.system :as system]
   [eventlog.views.eventviewer.toolbar :as toolbar]
   [reagent.core :as r]
   [reagent.dom :as rdom]))

(defn px [n]
  (str n "px"))

(defn root-style [{:keys [right-open? bottom-open? right-size bottom-size]}]
  {:--right-pane-width (if right-open?
                         (px (state/clamp right-size 240 520))
                         (px (:right-collapsed-size storage/default-layout)))
   :--bottom-pane-height (if bottom-open?
                           (px (state/clamp bottom-size 140 420))
                           (px (:bottom-collapsed-size storage/default-layout)))})

(defn center-row-style [{:keys [right-open? right-size]}]
  {:grid-template-columns
   (if right-open?
     (str "minmax(0, 1fr) 6px " (px (state/clamp right-size 240 520)))
     (str "minmax(0, 1fr) 6px " (px (:right-collapsed-size storage/default-layout))))})

(defn center-stack-style [{:keys [bottom-open? bottom-size]}]
  {:grid-template-rows
   (if bottom-open?
     (str "minmax(0, 1fr) 6px " (px (state/clamp bottom-size 140 420)))
     (str "minmax(0, 1fr) 6px " (px (:bottom-collapsed-size storage/default-layout))))})

(defn workspace [{:keys [layout-store drag-controller log-state]}]
  (let [{:keys [right-open? bottom-open? main-view] :as layout} @(storage/layout-state layout-store)]
    [:div.workspace
     [:div.workspace-main
      [:div.center-stack {:id "center-stack"
                          :style (center-stack-style layout)}
       [:div.center-row {:id "center-row"
                         :style (center-row-style layout)}
        [panes/main-panel {:layout-store layout-store
                           :drag-controller drag-controller
                           :main-view main-view
                           :log-state log-state}]
        [panes/vertical-splitter {:drag-controller drag-controller}]
        (if right-open?
          [panes/details-pane {:layout-store layout-store}]
          [panes/collapsed-details-rail {:layout-store layout-store}])]
       [panes/horizontal-splitter {:drag-controller drag-controller}]
       (if bottom-open?
         [panes/search-pane {:layout-store layout-store}]
         [panes/collapsed-search-rail {:layout-store layout-store}])]]]))

(defn shell [app-system]
  (let [layout @(storage/layout-state (:layout-store app-system))]
    [:div.app-shell {:style (root-style layout)}
     [toolbar/navbar {:layout-store (:layout-store app-system)}]
     [workspace app-system]]))

(defn app-root []
  (r/create-class
   {:display-name "eventviewer-root"
    :component-will-unmount
    (fn [_]
      (system/stop-system!))
    :reagent-render
    (fn []
      (if-let [app-system @system/system*]
        [shell app-system]
        [:div "Starting..."]))}))

(defn mount! []
  (rdom/render [app-root]
               (.getElementById js/document "app")))

(defn ^:export init []
  (system/stop-system!)
  (system/start-system!)
  (mount!))
