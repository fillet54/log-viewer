(ns eventlog.views.eventviewer.drag
  (:require
   [eventlog.storage :as storage]
   [re-frame.core :as rf]
   [reagent.core :as r]))

(defprotocol IDragController
  (install! [this])
  (uninstall! [this])
  (begin-drag! [this kind event])
  (end-drag! [this]))

(defn clamp [value min-value max-value]
  (-> value
      (max min-value)
      (min max-value)))

(defn element-bounds [id]
  (some-> (.getElementById js/document id) (.getBoundingClientRect)))

(defn set-user-select! [value]
  (set! (.. js/document -body -style -userSelect) value))

(defn calc-right-size [bounds event & [{:keys [collapsed-size]
                                        :or {collapsed-size (:right-collapsed-size storage/default-layout)}}]]
  (let [right-edge (+ (.-left bounds) (.-width bounds))
        next-size (- right-edge (.-clientX event))]
    (max collapsed-size next-size)))

(defn calc-bottom-size [bounds event & [{:keys [collapsed-size]
                                         :or {collapsed-size (:bottom-collapsed-size storage/default-layout)}}]]
  (let [bottom-edge (+ (.-top bounds) (.-height bounds))
        next-size (- bottom-edge (.-clientY event))]
    (max collapsed-size next-size)))

(defn calc-split-chart-size [bounds event]
  (let [top (.-top bounds)
        height (.-height bounds)
        next-size (- (.-clientY event) top)
        percent (* (/ next-size height) 100)]
    (clamp percent 20 80)))

(defn update-layout! [f & args]
  (rf/dispatch-sync (into [:eventviewer/update-layout f] args)))

(defn apply-resize! [value {:keys [min-size max-size open-key size-key last-size-key]}]
  (let [next-size (clamp value min-size max-size)]
    (update-layout!
     assoc
     open-key true
     size-key next-size
     last-size-key next-size)))

(defn apply-auto-collapse! [_value {:keys [min-size open-key size-key last-size-key]}]
  (update-layout!
   (fn [layout]
     (assoc layout
            open-key false
            last-size-key (get layout size-key min-size)))))

(defn make-auto-collapse-applier [{:keys [collapse-threshold] :as pane-config}]
  (fn [value]
    (if (< value collapse-threshold)
      (apply-auto-collapse! value pane-config)
      (apply-resize! value pane-config))))

(def drag-containers
  {:right {:bounds-id "center-row"
           :calc-value #(calc-right-size %1 %2 {:collapsed-size (:right-collapsed-size storage/default-layout)})
           :apply-value! (make-auto-collapse-applier
                          {:collapse-threshold 170
                           :min-size 240
                           :max-size 520
                           :open-key :right-open?
                           :size-key :right-size
                           :last-size-key :right-last-size})}
   :bottom {:bounds-id "center-stack"
            :calc-value #(calc-bottom-size %1 %2 {:collapsed-size (:bottom-collapsed-size storage/default-layout)})
            :apply-value! (make-auto-collapse-applier
                           {:collapse-threshold 96
                            :min-size 140
                            :max-size 420
                            :open-key :bottom-open?
                            :size-key :bottom-size
                            :last-size-key :bottom-last-size})}
   :split-chart {:bounds-id "split-stack"
                 :calc-value calc-split-chart-size
                 :apply-value! (fn [value]
                                 (update-layout! assoc :split-chart-size value))}})

(defn apply-drag! [kind event]
  (when-let [{:keys [bounds-id calc-value apply-value!]} (get drag-containers kind)]
    (when-let [bounds (element-bounds bounds-id)]
      (when-let [value (calc-value bounds event)]
        (apply-value! value)))))

(defrecord DragController [drag-state pointermove-fn pointerup-fn]
  IDragController
  (install! [this]
    (.addEventListener js/window "pointermove" pointermove-fn)
    (.addEventListener js/window "pointerup" pointerup-fn)
    this)
  (uninstall! [this]
    (end-drag! this)
    (.removeEventListener js/window "pointermove" pointermove-fn)
    (.removeEventListener js/window "pointerup" pointerup-fn)
    this)
  (begin-drag! [_ kind event]
    (.preventDefault event)
    (.stopPropagation event)
    (set-user-select! "none")
    (reset! drag-state {:kind kind}))
  (end-drag! [_]
    (set-user-select! "")
    (reset! drag-state nil)))

(defn make-drag-controller []
  (let [controller* (atom nil)
        drag-state (r/atom nil)
        pointermove-fn
        (fn [event]
          (when-let [{:keys [kind]} @drag-state]
            (apply-drag! kind event)))
        pointerup-fn
        (fn [_event]
          (when-let [controller @controller*]
            (end-drag! controller)))
        controller
        (->DragController drag-state pointermove-fn pointerup-fn)]
    (reset! controller* controller)
    controller))
