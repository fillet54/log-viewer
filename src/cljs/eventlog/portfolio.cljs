(ns eventlog.portfolio
  (:require
   [eventlog.app :as app]
   [eventlog.portfolio.shell-scenes]
   [portfolio.ui :as ui]
   [replicant.dom :as r]))

(defn ^:export init []
  (r/set-dispatch! (app/dispatch-for-store app/store))
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
       :value {:viewport/width 1480
               :viewport/height 900
               :viewport/padding [0 0 0 0]}}
      {:title "Desktop"
       :value {:viewport/width 1240
               :viewport/height 820
               :viewport/padding [0 0 0 0]}}
      {:title "Narrow"
       :value {:viewport/width 960
               :viewport/height 780
               :viewport/padding [0 0 0 0]}}]}}))
