(ns eventlog.views.eventviewer.toolbar
  (:require
   [clojure.string :as str]
   [eventlog.views.eventviewer.icons :as icons]
   [eventlog.views.eventviewer.state :as state]
   [re-frame.core :as rf]))

(defn toolbar-button [{:keys [view label active-view]}]
  [:button.toolbar-button
   {:class (when (= view active-view) "is-active")
    :type "button"
    :on-click #(rf/dispatch [:eventviewer/update-layout state/set-main-view view])}
   label])

(defn navbar []
  [:header.topbar
   [:div.brand
    [:span.brand-mark "EL"]
    [:div
     [:div.brand-title "Event Log"]
     [:div.brand-subtitle "Viewer workspace"]]]
   [:div.topbar-actions
    [:button.chrome-button
     {:type "button"
      :on-click #(rf/dispatch [:eventviewer/reset-layout])}
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

(defn main-toolbar [{:keys [active-view log-state]}]
  (let [{:keys [status events]} log-state]
    [:div.main-toolbar
     [:div.toolbar-group
      [toolbar-button {:view :list :label "List" :active-view active-view}]
      [toolbar-button {:view :chart :label "Chart" :active-view active-view}]
      [toolbar-button {:view :split :label "Split" :active-view active-view}]]
     [:div.toolbar-status
      [:span.status-pill (case status
                           :loading "Loading"
                           :error "Error"
                           "Core Event")]
      [:span.status-text
       (str "View: " (str/capitalize (name active-view))
            "  "
            (count events) " rows")]]]))
