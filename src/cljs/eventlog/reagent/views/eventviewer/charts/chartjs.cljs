(ns eventlog.reagent.views.eventviewer.charts.chartjs
  (:require
   [reagent.core :as r]))


(defn chart-instance []
  (some-> js/window .-Chart))

(defn resize-target [host]
  (or (some-> host (.closest ".chart-panel-body"))
      host))

(defn target-size [host]
  (when-let [element (resize-target host)]
    (let [rect (.getBoundingClientRect element)]
      {:width (max 0 (- (.-width rect) 2))
       :height (max 0 (- (.-height rect) 8))})))

(defn chart-canvas
  [{:keys [config class-name canvas-attrs host-attrs on-chart-ready on-host-ready]}]
  (let [canvas* (atom nil)
        host* (atom nil)
        chart* (atom nil)
        resize-observer* (atom nil)
        resize-listener* (atom nil)]
    (r/create-class
     {:display-name "chartjs-canvas"
      :component-did-mount
      (fn [_]
        (when on-host-ready
          (on-host-ready @host*))
        (when-let [Chart (chart-instance)]
          (reset! chart* (Chart. @canvas* (clj->js config)))
          (when on-chart-ready
            (on-chart-ready @chart*))
          (let [resize-fn (fn []
                            (when-let [chart @chart*]
                              (let [{:keys [width height]} (target-size @host*)]
                                (.resize chart width height)
                                (.update chart "none"))))]
            (.addEventListener js/window "eventviewer:layout-resized" resize-fn)
            (reset! resize-listener* resize-fn))
          (when (exists? js/ResizeObserver)
            (let [observer (js/ResizeObserver.
                            (fn [_entries]
                              (when-let [chart @chart*]
                                (let [{:keys [width height]} (target-size @host*)]
                                  (.resize chart width height)
                                  (.update chart "none")))))]
              (.observe observer (resize-target @host*))
              (reset! resize-observer* observer)))))
      :component-did-update
      (fn [_ _]
        (when-let [chart @chart*]
          (set! (.-data chart) (clj->js (:data config)))
          (set! (.-options chart) (clj->js (:options config)))
          (set! (.-plugins chart) (clj->js (or (:plugins config) [])))
          (.update chart)))
      :component-will-unmount
      (fn [_]
        (when on-chart-ready
          (on-chart-ready nil))
        (when on-host-ready
          (on-host-ready nil))
        (when-let [listener @resize-listener*]
          (.removeEventListener js/window "eventviewer:layout-resized" listener)
          (reset! resize-listener* nil))
        (when-let [observer @resize-observer*]
          (.disconnect observer)
          (reset! resize-observer* nil))
        (when-let [chart @chart*]
          (.destroy chart)
          (reset! chart* nil)))
      :reagent-render
      (fn [_]
        (if (chart-instance)
          [:div.chartjs-host
           (merge
            {:ref #(reset! host* %)}
            host-attrs)
           [:canvas
            (merge
             {:class class-name
              :ref #(reset! canvas* %)
              :role "img"
              :aria-label "Event histogram chart"}
             canvas-attrs)]]
          [:div.placeholder-pane
           [:div.placeholder-label "Chart.js Not Loaded"]]))})))
