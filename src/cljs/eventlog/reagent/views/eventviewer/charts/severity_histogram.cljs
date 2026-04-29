(ns eventlog.reagent.views.eventviewer.charts.severity-histogram
  (:require
   [eventlog.reagent.views.eventviewer.charts.chartjs :as chartjs]
   [eventlog.reagent.views.eventviewer.events.base :as event-base]
   [reagent.core :as r]
   [re-frame.core :as rf]))

(def severity-order
  [{:severity "Green" :label "Green" :color "rgba(34, 197, 94, 0.85)"}
   {:severity "Yellow" :label "Yellow" :color "rgba(250, 204, 21, 0.9)"}
   {:severity "Red" :label "Red" :color "rgba(239, 68, 68, 0.9)"}
   {:severity "Flashing Red" :label "Flashing Red" :color "rgba(127, 29, 29, 0.95)"}])

(def histogram-bin-count 72)

(def tooltip-time-formatter
  (js/Intl.DateTimeFormat. "en-US"
                           #js {:timeZone "UTC"
                                :year "numeric"
                                :month "2-digit"
                                :day "2-digit"
                                :hour "2-digit"
                                :minute "2-digit"
                                :second "2-digit"
                                :hour12 false}))

(def axis-time-formatter
  (js/Intl.DateTimeFormat. "en-US"
                           #js {:timeZone "UTC"
                                :hour "2-digit"
                                :minute "2-digit"
                                :hour12 false}))

(defn format-axis-time [utc-ms]
  (.format axis-time-formatter (js/Date. utc-ms)))

(defn format-hover-time [utc-ms]
  (when (number? utc-ms)
    (str (.format tooltip-time-formatter (js/Date. utc-ms)) " UTC")))

(defn set-events [events]
  (filter :is_set events))

(defn histogram-domain [events]
  (let [set-events (vec (set-events events))
        times (vec (keep event-base/event-time-ms set-events))
        [min-time max-time] (if (seq times)
                              [(apply min times) (apply max times)]
                              [0 0])]
    {:events set-events
     :min-time min-time
     :max-time max-time}))

(defn bin-width [{:keys [min-time max-time]}]
  (max 1 (/ (max 1 (- max-time min-time)) histogram-bin-count)))

(defn event->bin-index [{:keys [min-time max-time] :as domain} event]
  (when-let [event-time (event-base/event-time-ms event)]
    (let [width (bin-width domain)
          idx (int (js/Math.floor (/ (- event-time min-time) width)))]
      (-> idx
          (max 0)
          (min (dec histogram-bin-count))))))

(defn empty-bin [index start end]
  {:index index
   :start start
   :end end
   :center (+ start (/ (- end start) 2))
   :counts {"Green" 0
            "Yellow" 0
            "Red" 0
            "Flashing Red" 0}})

(defn build-bins [events]
  (let [{:keys [events min-time max-time] :as domain} (histogram-domain events)
        width (bin-width domain)
        bins (vec
              (for [idx (range histogram-bin-count)
                    :let [start (+ min-time (* idx width))
                          end (if (= idx (dec histogram-bin-count))
                                (+ max-time 1)
                                (+ start width))]]
                (empty-bin idx start end)))]
    (reduce
     (fn [acc event]
       (if-let [idx (event->bin-index domain event)]
         (update-in acc [idx :counts (:severity event)] (fnil inc 0))
         acc))
     bins
     events)))

(defn dataset-for [bins {:keys [severity label color]}]
  {:label label
   :data (mapv #(get-in % [:counts severity] 0) bins)
   :backgroundColor color
   :borderColor color
   :borderWidth 1
   :borderSkipped false
   :stack "event-counts"
   :barPercentage 1.0
   :categoryPercentage 1.0})

(defn closest-event-at-time [events target-time]
  (apply min-key #(js/Math.abs (- (event-base/event-time-ms %) target-time)) events))

(defn interpolate [value in-min in-max out-min out-max]
  (let [in-span (- in-max in-min)]
    (if (zero? in-span)
      out-min
      (+ out-min (* (/ (- value in-min) in-span)
                    (- out-max out-min))))))

(defn hover-time-from-event [chart {:keys [min-time max-time]} event]
  (let [x (.-x event)
        y (.-y event)
        chart-area (.-chartArea chart)]
    (when (and chart-area
               (<= (.-left chart-area) x (.-right chart-area))
               (<= (.-top chart-area) y (.-bottom chart-area)))
      (interpolate x
                   (.-left chart-area)
                   (.-right chart-area)
                   min-time
                   max-time))))

(defn hover-x-from-time [chart {:keys [min-time max-time]} hover-time]
  (let [chart-area (some-> chart .-chartArea)]
    (when (and chart-area (number? hover-time))
      (interpolate hover-time
                   min-time
                   max-time
                   (.-left chart-area)
                   (.-right chart-area)))))

(defn histogram-config [events on-hover-time]
  (let [{:keys [min-time max-time] :as domain} (histogram-domain events)
        bins (build-bins events)
        labels (mapv (fn [{:keys [start end]}]
                       (str (format-axis-time start) " - " (format-axis-time end)))
                     bins)
        datasets (mapv #(dataset-for bins %) severity-order)
        events (vec (set-events events))]
    {:type "bar"
     :data {:labels labels
            :datasets datasets}
     :options {:responsive true
               :maintainAspectRatio false
               :animation false
               :interaction {:mode "index"
                             :intersect true}
               :plugins {:legend {:position "top"
                                  :align "start"}
                         :tooltip {:callbacks {:title (fn [items]
                                                        (some-> items
                                                                first
                                                                (aget "label")))
                                               :label (fn [context]
                                                        (str (aget context "dataset" "label")
                                                             ": "
                                                             (aget context "formattedValue")))}}}
               :scales {:x {:stacked true
                            :ticks {:maxRotation 0
                                    :autoSkip true
                                    :maxTicksLimit 8}}
                        :y {:stacked true
                            :beginAtZero true
                            :title {:display true
                                    :text "SET event count"}}}
               :onHover (fn [event _elements chart]
                          (on-hover-time (hover-time-from-event chart domain event)))
               :onClick (fn [event _elements chart]
                          (when-let [target-time (hover-time-from-event chart domain event)]
                            (when-let [closest-event (closest-event-at-time events target-time)]
                              (rf/dispatch [:eventviewer/select-event (:row-id closest-event)]))))} }))

(defn component [{:keys [events viewport-time]}]
  (r/with-let [chart* (atom nil)
               host* (atom nil)
               hover-time* (r/atom nil)
               clear-hover! #(reset! hover-time* nil)
               handle-window-pointer-move!
               (fn [event]
                 (when @hover-time*
                   (if-let [host @host*]
                     (let [rect (.getBoundingClientRect host)
                           x (.-clientX event)
                           y (.-clientY event)
                           inside? (and (<= (.-left rect) x (.-right rect))
                                        (<= (.-top rect) y (.-bottom rect)))]
                       (when-not inside?
                         (clear-hover!)))
                     (clear-hover!))))
               handle-window-mouse-out!
               (fn [event]
                 (when-not (.-relatedTarget event)
                   (clear-hover!)))]
    (.addEventListener js/window "pointermove" handle-window-pointer-move!)
    (.addEventListener js/window "blur" clear-hover!)
    (.addEventListener js/window "mouseout" handle-window-mouse-out!)
    (let [domain (histogram-domain events)
          chart @chart*
          hover-time @hover-time*
          chart-area (some-> chart .-chartArea)
          active-time (or hover-time viewport-time)
          active-kind (if hover-time :hover :viewport)
          active-x (hover-x-from-time chart domain active-time)
          active-visible? (and active-x chart-area
                              (<= (.-left chart-area) active-x (.-right chart-area)))]
      [:div.timeline-chart-shell
       [chartjs/chart-canvas
        {:class-name "chartjs-canvas chartjs-severity-histogram"
         :config (histogram-config events #(reset! hover-time* %))
         :on-chart-ready #(reset! chart* %)
         :on-host-ready #(reset! host* %)
         :host-attrs {:on-mouse-leave clear-hover!
                      :on-pointer-leave clear-hover!
                      :on-mouse-out clear-hover!}
         :canvas-attrs {:on-mouse-leave clear-hover!
                        :on-pointer-leave clear-hover!}}]
       [:div.timeline-indicator-overlay
        {:class (case active-kind
                  :viewport "is-viewport"
                  "is-hover")
         :hidden (not active-visible?)
         :style (when active-visible?
                  {:left (str active-x "px")
                   :top (str (.-top chart-area) "px")
                   :height (str (- (.-bottom chart-area) (.-top chart-area)) "px")})}
        [:div.timeline-indicator-line]
        (when hover-time
          [:div.timeline-indicator-label (format-hover-time hover-time)])]])
    (finally
      (.removeEventListener js/window "pointermove" handle-window-pointer-move!)
      (.removeEventListener js/window "blur" clear-hover!)
      (.removeEventListener js/window "mouseout" handle-window-mouse-out!))))
