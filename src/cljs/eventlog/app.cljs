(ns eventlog.app
  (:require
   [eventlog.views.eventviewer.layout :as layout]
   [replicant.dom :as r]))

(defonce store (atom (layout/default-state)))

(defn app-element []
  (or (.getElementById js/document "app")
      (.getElementById js/document "replicant-app")))

(defn render! [state]
  (when-let [el (app-element)]
    (r/render el (layout/app-shell {:store store :state state}))))

(defn ^:export init []
  (r/set-dispatch! layout/dispatch!)
  (remove-watch store ::render)
  (add-watch store ::render (fn [_ _ _ state] (render! state)))
  (reset! store (layout/default-state))
  (render! @store))
