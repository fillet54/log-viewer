(ns eventlog.views.eventviewer.events.core-event
  (:require
   [clojure.string :as str]))

(def channel-labels ["A" "B" "C" "D"])

(defn severity-class [severity]
  (case severity
    "Green" "log-green"
    "Yellow" "log-yellow"
    "Red" "log-red"
    "Flashing Red" "log-flashing-red"
    ""))

(defn severity-token [severity]
  (-> severity
      str/lower-case
      (str/replace " " "-")))

(defn action-contrast [severity]
  (if (= severity "Yellow") "dark" "light"))

(defn location-label [{:keys [system subsystem unit]}]
  (str (second system) " / " (second subsystem) " / " (second unit)))

(defn channel-active? [channels label]
  (not= -1 (.indexOf channels label)))

(defn log-row [event {:keys [selected? on-select]}]
  [:article.log-line
   {:class [(severity-class (:severity event))
            (when selected? "is-selected")]
    :data-set-clear (if (:is_set event) "set" "clear")
    :on-click on-select}
   [:span.log-channels
    (for [label channel-labels]
      ^{:key label}
      [:span.log-channel {:class (when (channel-active? (:channels event) label) "is-on")}
       label])]
   [:span.log-time.log-muted (:utctime event)]
   [:span.log-offset.log-muted (str "T+" (:time event) "s")]
   [:span.log-action
    {:data-contrast (action-contrast (:severity event))
     :data-event-color (severity-token (:severity event))}
    [:span.log-action-label (if (:is_set event) "SET" "CLEAR")]]
   [:span.log-details
    [:span.log-title-row
     [:span.log-name (:name event)]
     [:span.log-meta (location-label event)]]
    [:span.log-desc (:description event)]]])
