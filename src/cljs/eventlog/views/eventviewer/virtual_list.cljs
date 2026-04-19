(ns eventlog.views.eventviewer.virtual-list
  (:require
   [eventlog.views.eventviewer.events.base :as events]
   [reagent.core :as r]))

(defn clamp [n low high]
  (max low (min high n)))

(defn virtual-list
  [{:keys [rows row-height overscan render-row selected-row-id on-select]
    :or {row-height 42
         overscan 8}}]
  (r/with-let [scroll-top* (r/atom 0)
               viewport-h* (r/atom 400)
               container* (atom nil)]
    (let [rows (vec rows)
          row-count (count rows)
          scroll-top @scroll-top*
          viewport-h @viewport-h*
          total-height (* row-count row-height)
          raw-start (int (js/Math.floor (/ scroll-top row-height)))
          start-idx (clamp (- raw-start overscan) 0 row-count)
          visible-count (int (js/Math.ceil (/ viewport-h row-height)))
          end-idx (clamp (+ raw-start visible-count overscan) 0 row-count)
          top-pad (* start-idx row-height)
          bottom-pad (* (- row-count end-idx) row-height)
          visible-rows (subvec rows start-idx end-idx)]
      [:div.log-list
       {:ref (fn [element]
               (reset! container* element)
               (when element
                 (reset! viewport-h* (.-clientHeight element))))
        :on-scroll (fn [event]
                     (let [element (.-target event)]
                       (reset! scroll-top* (.-scrollTop element))
                       (reset! viewport-h* (.-clientHeight element))))}
       [:div.log-list-virtual
        {:style {:height (str total-height "px")}}
        [:div {:style {:height (str top-pad "px")}}]
        (doall
          (map-indexed
          (fn [local-idx row]
            (let [idx (+ start-idx local-idx)]
              ^{:key (str idx "|" (events/event-time row))}
              [:div.log-list-row
               {:style {:height (str row-height "px")}}
               [render-row row {:index idx
                                :selected? (= (:row-id row) selected-row-id)
                                :on-select #(on-select row)}]]))
          visible-rows))
        [:div {:style {:height (str bottom-pad "px")}}]]])))
