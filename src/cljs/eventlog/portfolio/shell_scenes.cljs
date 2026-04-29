(ns eventlog.portfolio.shell-scenes
  (:require
   [eventlog.app :as app]
   [eventlog.views.eventviewer.navbar :as navbar]
   [eventlog.views.eventviewer.shell :as shell]
   [portfolio.replicant :as portfolio :refer-macros [defscene]]
   [replicant.dom :as r]))

(portfolio/configure-scenes
 {:title "Event Viewer Shell"
  :idx 10})

(defscene navbar-component
  "Standalone top navigation for the event viewer shell."
  :title "Navbar"
  :params (atom {
      :app/title "Log Viewer"
      :nav/home-href "/"
      :log/source "prod-api-gateway"
      :log/start-date "Apr 12"
      :log/end-date "Apr 26, 2026"
      :log/row-count "2.4M events"
      :log/level-summary "Errors + Warnings"
      :user/name "Phillip"
      :user/email "phillip@example.com"
      :user/initials "PG"
      :user/avatar-url nil})
  :on-mount #(r/set-dispatch! (app/dispatch-for-store %))
  [store]
  (navbar/navbar @store))

(defscene expanded-shell
  "Navbar and shell with both secondary panes open. Splitters and collapse buttons are interactive."
  :title "Expanded"
  :params (atom (shell/default-state))
  :on-mount #(r/set-dispatch! (app/dispatch-for-store %))
  [store]
  (shell/app-shell {:state @store}))

(defscene right-collapsed-shell
  "The details pane is collapsed into its right rail. Click the rail to reopen it."
  :title "Details Collapsed"
  :params (atom (update (shell/default-state) :layout assoc :right-open? false))
  :on-mount #(r/set-dispatch! (app/dispatch-for-store %))
  [store]
  (shell/app-shell {:state @store}))

(defscene bottom-collapsed-shell
  "The search pane is collapsed into its bottom rail. Click the rail to reopen it."
  :title "Search Collapsed"
  :params (atom (update (shell/default-state) :layout assoc :bottom-open? false))
  :on-mount #(r/set-dispatch! (app/dispatch-for-store %))
  [store]
  (shell/app-shell {:state @store}))

(defscene fully-collapsed-shell
  "Both secondary panes are collapsed, showing the compact rail states."
  :title "Fully Collapsed"
  :params (atom (shell/collapsed-state))
  :on-mount #(r/set-dispatch! (app/dispatch-for-store %))
  [store]
  (shell/app-shell {:state @store}))
