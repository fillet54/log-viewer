(ns eventlog.views.eventviewer.state)

(defn clamp [value min-value max-value]
  (-> value
      (max min-value)
      (min max-value)))

(defn set-main-view [layout view]
  (assoc layout :main-view view))

(defn toggle-pane [layout pane-key size-key default-size]
  (let [open? (get layout pane-key)
        last-size-key (case size-key
                        :right-size :right-last-size
                        :bottom-size :bottom-last-size
                        nil)
        size (or (get layout last-size-key) (get layout size-key) default-size)]
    (if open?
      (cond-> (assoc layout pane-key false)
        last-size-key (assoc last-size-key (get layout size-key default-size)))
      (cond-> (assoc layout pane-key true size-key size)
        last-size-key (assoc last-size-key size)))))
