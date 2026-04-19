(ns eventlog.views.eventviewer.toolbar
  (:require
   [clojure.string :as str]
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.icons :as icons]
   [eventlog.views.eventviewer.state :as state]))

(defn toolbar-button [{:keys [layout-store view label active-view]}]
  [:button.toolbar-button
   {:class (when (= view active-view) "is-active")
    :type "button"
    :on-click #(storage/swap-layout! layout-store state/set-main-view view)}
   label])

(defn navbar [{:keys [layout-store]}]
  [:header.topbar
   [:div.brand
    [:span.brand-mark "EL"]
    [:div
     [:div.brand-title "Event Log"]
     [:div.brand-subtitle "Viewer workspace"]]]
   [:div.topbar-actions
    [:button.chrome-button
     {:type "button"
      :on-click #(storage/reset-layout! layout-store)}
     "Reset layout"]]])

(defn panel-header [title subtitle actions]
  [:div.panel-header
   [:div
    [:h2.panel-title title]
    (when subtitle [:p.panel-subtitle subtitle])]
   (when actions
     [:div.panel-actions actions])])

(defn pane-toggle-button [{:keys [title on-click icon]}]
  [:button.pane-toggle-button
   {:type "button"
    :title title
    :aria-label title
    :on-click on-click}
   [icons/chevron-icon icon]])

(defn main-toolbar [{:keys [layout-store active-view]}]
  [:div.main-toolbar
   [:div.toolbar-group
    [toolbar-button {:layout-store layout-store :view :list :label "List" :active-view active-view}]
    [toolbar-button {:layout-store layout-store :view :chart :label "Chart" :active-view active-view}]
    [toolbar-button {:layout-store layout-store :view :split :label "Split" :active-view active-view}]]
   [:div.toolbar-status
    [:span.status-pill "SPA shell"]
    [:span.status-text
     (str "View: " (str/capitalize (name active-view)))]]])
