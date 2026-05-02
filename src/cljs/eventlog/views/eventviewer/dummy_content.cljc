(ns eventlog.views.eventviewer.dummy-content)

(defn placeholder [label]
  [:div.placeholder-pane
   [:div.engraved-pane-label label]])

(defn main-view [_props]
  (placeholder "Main"))

(defn side-view [_props]
  (placeholder "Right"))

(defn bottom-view [_props]
  (placeholder "Bottom"))

(def default-views
  {:list main-view
   :chart main-view
   :details side-view
   :search bottom-view})
