(ns eventlog.portfolio.layout-scenes
  (:require
   [eventlog.views.eventviewer.layout :as layout]
   [portfolio.replicant :as portfolio :refer-macros [defscene]]))

(portfolio/configure-scenes
 {:title "Replicant Layout"
  :idx 10})

(defscene expanded-layout
  "Navbar and body layout with both secondary panes open. Splitters and collapse buttons are interactive."
  :title "Expanded"
  :params (atom (layout/default-state))
  [store]
  (layout/app-shell {:store store :state @store}))

(defscene right-collapsed-layout
  "The details pane is collapsed into its right rail. Click the rail to reopen it."
  :title "Details Collapsed"
  :params (atom (update (layout/default-state) :layout assoc :right-open? false))
  [store]
  (layout/app-shell {:store store :state @store}))

(defscene bottom-collapsed-layout
  "The search pane is collapsed into its bottom rail. Click the rail to reopen it."
  :title "Search Collapsed"
  :params (atom (update (layout/default-state) :layout assoc :bottom-open? false))
  [store]
  (layout/app-shell {:store store :state @store}))

(defscene fully-collapsed-layout
  "Both secondary panes are collapsed, showing the compact rail states."
  :title "Fully Collapsed"
  :params (atom (layout/collapsed-state))
  [store]
  (layout/app-shell {:store store :state @store}))
