(ns eventlog.app
  (:require
   [reagent.dom :as rdom]))

(defn hello-world []
  [:main.page
   [:div.hero
    [:p.kicker "Event Log Viewer"]
    [:h1 "Hello world"]
    [:p "Reagent is mounted and the Ring/Reitit server is serving this page."]]])

(defn mount! []
  (when-let [root (.getElementById js/document "app")]
    (rdom/render [hello-world] root)))

(defn ^:export init []
  (mount!))
