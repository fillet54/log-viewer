(ns ^:figwheel-hooks eventlog.app
  (:require
   [clojure.walk :as walk]
   [eventlog.views.eventviewer.shell :as shell]
   [eventlog.views.eventviewer.navbar :refer [navbar]]
   [replicant.dom :as r]))

(defonce store (atom (shell/default-state)))

(defn app-element []
  (or (.getElementById js/document "eventlog-app")
      (.getElementById js/document "replicant-app")))

(defn render-ui [state]
  [:div
    (navbar state)
    (shell/app-shell {:state state})])

(defn render! [state]
  (when-let [el (app-element)]
    (r/render el (render-ui state))))

(defn event->pointer [event]
  (when event
    {:client-x (.-clientX event)
     :client-y (.-clientY event)}))

(defn element-bounds [id]
  (when-let [element (.getElementById js/document id)]
    (let [bounds (.getBoundingClientRect element)]
      {:left (.-left bounds)
       :top (.-top bounds)
       :width (.-width bounds)
       :height (.-height bounds)})))

(defn set-user-select! [value]
  (set! (.. js/document -body -style -userSelect) value))

(declare handle-actions)

(defn begin-drag! [store kind event]
  (.preventDefault event)
  (.stopPropagation event)
  (set-user-select! "none")
  (let [move-handler
        (fn [move-event]
          (when-let [{:keys [bounds-id]} (get shell/pane-configs kind)]
            (when-let [bounds (element-bounds bounds-id)]
              (when-let [value (shell/drag-value kind bounds (event->pointer move-event))]
                (handle-actions store move-event [[:layout/resize kind value]])))))
        up-handler* (atom nil)
        up-handler (fn [_up-event]
                     (set-user-select! "")
                     (.removeEventListener js/window "pointermove" move-handler)
                     (when-let [handler @up-handler*]
                       (.removeEventListener js/window "pointerup" handler)))]
    (reset! up-handler* up-handler)
    (.addEventListener js/window "pointermove" move-handler)
    (.addEventListener js/window "pointerup" up-handler)))

(defn process-effect [store [effect & args]]
  (case effect
    :effect/assoc-in
    (apply swap! store assoc-in args)

    :effect/update-in
    (let [[path f & f-args] args]
     (apply swap! store update-in path f f-args))

    :effect/begin-drag
    (let [[kind event] args]
      (begin-drag! store kind event))

    nil))

(defn perform-actions [state actions]
  (mapcat
   (fn [action]
     (or (shell/perform-action state action)
         (js/console.warn "Unknown action" (pr-str action))))
   actions))

(defn interpolate [event data]
  (walk/postwalk
   (fn [x]
     (case x
       :event/dom-event
       event

       :event.target/value
       (some-> event .-target .-value)

       :event.target/value-as-number
       (some-> event .-target .-valueAsNumber)

       :event.target/value-as-keyword
       (some-> event .-target .-value keyword)

       x))
   data))

(defn handle-actions [store dom-event actions]
  (->> (interpolate dom-event actions)
       (perform-actions @store)
       (run! #(process-effect store %))))

(defn dispatch-for-store [store]
  (fn [{:replicant/keys [dom-event]} actions]
    (handle-actions store dom-event actions)))

(defn mount! []
  (r/set-dispatch! (dispatch-for-store store))
  (remove-watch store ::render)
  (add-watch store ::render (fn [_ _ _ state] (render! state)))
  (render! @store))

(defn ^:export init []
  (reset! store (shell/default-state))
  (mount!))

(defn ^:export ^:after-load on-code-reload []
  (mount!))
