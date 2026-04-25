(ns eventlog.reagent.views.eventviewer.root
  (:require
   [eventlog.storage :as storage]
   [eventlog.reagent.views.eventviewer.panes :as panes]
   [eventlog.reagent.views.eventviewer.state :as state]
   [eventlog.reagent.views.eventviewer.system :as system]
   [eventlog.reagent.views.eventviewer.toolbar :as toolbar]
   [re-frame.core :as rf]
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

(defn workspace [{:keys [drag-controller]}]
  (let [{:keys [right-open? bottom-open? main-view] :as layout} @(rf/subscribe [:eventviewer/layout])
        log-state @(rf/subscribe [:eventviewer/log])]
    [:div.workspace
     [:div.workspace-main
      [:div.center-stack {:id "center-stack"
                          :style (center-stack-style layout)}
       [:div.center-row {:id "center-row"
                         :style (center-row-style layout)}
        [panes/main-panel {:drag-controller drag-controller
                           :layout layout
                           :main-view main-view
                           :log-state log-state}]
        [panes/vertical-splitter {:drag-controller drag-controller}]
        (if right-open?
          [panes/details-pane {:layout layout
                               :log-state log-state}]
          [panes/collapsed-details-rail {:layout layout}])]
       [panes/horizontal-splitter {:drag-controller drag-controller}]
       (if bottom-open?
         [panes/search-pane {:layout layout}]
         [panes/collapsed-search-rail {:layout layout}])]]]))

(defn shell [app-system]
  (let [layout @(rf/subscribe [:eventviewer/layout])]
    [:div.app-shell {:style (root-style layout)}
     [toolbar/navbar]
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
