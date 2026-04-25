(ns eventlog.portfolio
  (:require
   [eventlog.portfolio.layout-scenes]
   [eventlog.views.eventviewer.layout :as layout]
   [portfolio.ui :as ui]
   [replicant.dom :as r]))

(defn ^:export init []
  (r/set-dispatch! layout/dispatch!)
  (ui/start!
   {:config
    {:css-paths ["/css/app.css"]
     :portfolio-docs? false
     :background/default-option-id :eventlog-light
     :background/options
     [{:id :eventlog-light
       :title "Event Log Light"
       :value {:background/background-color "#f4f7fb"}}]
     :viewport/options
     [{:title "Workspace"
       :value {:viewport/width "100%"
               :viewport/height 760
               :viewport/padding [0 0 0 0]}}
      {:title "Narrow"
       :value {:viewport/width 860
               :viewport/height 760
               :viewport/padding [0 0 0 0]}}]}}))
