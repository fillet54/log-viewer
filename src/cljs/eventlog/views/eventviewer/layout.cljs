(ns eventlog.views.eventviewer.layout
  (:require
   [clojure.string :as str]
   [eventlog.storage :as storage]
   [eventlog.views.eventviewer.dummy-content :as dummy-content]
   [eventlog.views.eventviewer.icons :as icons]))

(def layout-storage-key "eventlog.views.eventviewer.layout.v1")

(defn default-state []
  {:layout storage/default-layout})

(defn collapsed-state []
  (assoc (default-state)
         :layout (assoc storage/default-layout
                        :right-open? false
                        :bottom-open? false)))

(defn clamp [value min-value max-value]
  (-> value
      (max min-value)
      (min max-value)))

(defn px [n]
  (str n "px"))

(defn root-style [{:keys [right-open? bottom-open? right-size bottom-size]}]
  {"--right-pane-width" (if right-open?
                          (px (clamp right-size 240 520))
                          (px (:right-collapsed-size storage/default-layout)))
   "--bottom-pane-height" (if bottom-open?
                            (px (clamp bottom-size 140 420))
                            (px (:bottom-collapsed-size storage/default-layout)))})

(defn center-row-style [{:keys [right-open? right-size]}]
  {:grid-template-columns
   (if right-open?
     (str "minmax(0, 1fr) 6px " (px (clamp right-size 240 520)))
     (str "minmax(0, 1fr) 6px " (px (:right-collapsed-size storage/default-layout))))})

(defn center-stack-style [{:keys [bottom-open? bottom-size]}]
  {:grid-template-rows
   (if bottom-open?
     (str "minmax(0, 1fr) 6px " (px (clamp bottom-size 140 420)))
     (str "minmax(0, 1fr) 6px " (px (:bottom-collapsed-size storage/default-layout))))})

(defn split-stack-style [{:keys [split-chart-size]}]
  {:grid-template-rows (str (clamp split-chart-size 120 520) "px 6px minmax(0, 1fr)")})

(defn element-bounds [id]
  (some-> (.getElementById js/document id) (.getBoundingClientRect)))

(defn set-user-select! [value]
  (set! (.. js/document -body -style -userSelect) value))

(defn calc-right-size [bounds event]
  (let [right-edge (+ (.-left bounds) (.-width bounds))
        next-size (- right-edge (.-clientX event))]
    (max (:right-collapsed-size storage/default-layout) next-size)))

(defn calc-bottom-size [bounds event]
  (let [bottom-edge (+ (.-top bounds) (.-height bounds))
        next-size (- bottom-edge (.-clientY event))]
    (max (:bottom-collapsed-size storage/default-layout) next-size)))

(defn calc-split-chart-size [bounds event]
  (clamp (- (.-clientY event) (.-top bounds)) 120 520))

(def pane-configs
  {:right {:bounds-id "replicant-center-row"
           :calc-value calc-right-size
           :collapse-threshold 170
           :min-size 240
           :max-size 520
           :open-key :right-open?
           :size-key :right-size
           :last-size-key :right-last-size}
   :bottom {:bounds-id "replicant-center-stack"
            :calc-value calc-bottom-size
            :collapse-threshold 96
            :min-size 140
            :max-size 420
            :open-key :bottom-open?
            :size-key :bottom-size
            :last-size-key :bottom-last-size}
   :split-chart {:bounds-id "replicant-split-stack"
                 :calc-value calc-split-chart-size
                 :min-size 120
                 :max-size 520
                 :size-key :split-chart-size}})

(defn resize-layout [layout kind value]
  (if-let [{:keys [collapse-threshold min-size max-size open-key size-key last-size-key]} (pane-configs kind)]
    (if (= kind :split-chart)
      (assoc layout size-key (clamp value min-size max-size))
      (if (< value collapse-threshold)
        (assoc layout
               open-key false
               last-size-key (get layout size-key min-size))
        (let [next-size (clamp value min-size max-size)]
          (assoc layout
                 open-key true
                 size-key next-size
                 last-size-key next-size))))
    layout))

(defn set-main-view [layout view]
  (assoc layout :main-view view))

(defn toggle-pane [layout pane]
  (case pane
    :right (if (:right-open? layout)
             (assoc layout
                    :right-open? false
                    :right-last-size (:right-size layout))
             (assoc layout
                    :right-open? true
                    :right-size (:right-last-size layout)))
    :bottom (if (:bottom-open? layout)
              (assoc layout
                     :bottom-open? false
                     :bottom-last-size (:bottom-size layout))
              (assoc layout
                     :bottom-open? true
                     :bottom-size (:bottom-last-size layout)))
    layout))

(defn reset-layout! [store]
  (swap! store assoc :layout storage/default-layout))

(defn update-layout! [store f & args]
  (apply swap! store update :layout f args))

(defn apply-drag! [store kind event]
  (when-let [{:keys [bounds-id calc-value]} (pane-configs kind)]
    (when-let [bounds (element-bounds bounds-id)]
      (update-layout! store resize-layout kind (calc-value bounds event)))))

(defn begin-drag! [store kind event]
  (.preventDefault event)
  (.stopPropagation event)
  (set-user-select! "none")
  (let [move-handler (fn [move-event]
                       (apply-drag! store kind move-event))
        up-handler* (atom nil)
        up-handler (fn [_up-event]
                     (set-user-select! "")
                     (.removeEventListener js/window "pointermove" move-handler)
                     (when-let [handler @up-handler*]
                       (.removeEventListener js/window "pointerup" handler)))]
    (reset! up-handler* up-handler)
    (.addEventListener js/window "pointermove" move-handler)
    (.addEventListener js/window "pointerup" up-handler)))

(defn execute-action! [event-data [action & args]]
  (case action
    :layout/reset
    (let [[store] args]
      (reset-layout! store))

    :layout/set-main-view
    (let [[store view] args]
      (update-layout! store set-main-view view))

    :layout/toggle-pane
    (let [[store pane] args]
      (update-layout! store toggle-pane pane))

    :layout/begin-drag
    (let [[store kind] args]
      (begin-drag! store kind (:replicant/dom-event event-data)))

    nil))

(defn dispatch!
  ([event-data actions]
   (doseq [action actions]
     (execute-action! event-data action)))
  ([_event-data]
   nil))

(defn toolbar-button [{:keys [store view label active-view]}]
  [:button
   {:class (if (= view active-view)
             "toolbar-button is-active"
             "toolbar-button")
    :type "button"
    :on {:click [[:layout/set-main-view store view]]}}
   label])

(defn navbar [{:keys [store]}]
  [:header.topbar
   [:div.brand
    [:span.brand-mark "EL"]
    [:div
     [:div.brand-title "Event Log"]
     [:div.brand-subtitle "Replicant layout spike"]]]
   [:div.topbar-actions
    [:button.chrome-button
     {:type "button"
      :on {:click [[:layout/reset store]]}}
     "Reset layout"]]])

(defn panel-header [{:keys [title subtitle actions]}]
  [:div.panel-header
   [:div
    [:h2.panel-title title]
    (when subtitle
      [:p.panel-subtitle subtitle])]
   (when actions
     [:div.panel-actions actions])])

(defn pane-toggle-button [{:keys [title icon actions]}]
  [:button.pane-toggle-button
   {:type "button"
    :title title
    :aria-label title
    :on {:click actions}}
   (icons/chevron-icon icon)])

(defn main-toolbar [{:keys [store active-view]}]
  [:div.main-toolbar
   [:div.toolbar-group
    (toolbar-button {:store store :view :list :label "List" :active-view active-view})
    (toolbar-button {:store store :view :chart :label "Chart" :active-view active-view})
    (toolbar-button {:store store :view :split :label "Split" :active-view active-view})]
   [:div.toolbar-status
    [:span.status-pill "Mock"]
    [:span.status-text (str "View: " (str/capitalize (name active-view)) "  " "dummy rows")]]])

(defn render-view [views view-id props]
  (if-let [view (get views view-id)]
    (view props)
    [:div.placeholder-pane
     [:div.placeholder-label (str "Missing view: " (name view-id))]]))

(defn split-view [{:keys [store layout views]}]
  [:section.split-stack
   {:id "replicant-split-stack"
    :style (split-stack-style layout)}
   (render-view views :chart {:compact? true})
   [:div.splitter.splitter-inner
    {:on {:pointerdown [[:layout/begin-drag store :split-chart]]}}]
   (render-view views :list {})])

(defn main-panel [{:keys [store layout views]}]
  (let [main-view (:main-view layout)]
    [:main.main-panel
     (main-toolbar {:store store :active-view main-view})
     [:div.main-body
      (case main-view
        :chart (render-view views :chart {:compact? false})
        :split (split-view {:store store :layout layout :views views})
        (render-view views :list {}))]]))

(defn details-pane [{:keys [store views]}]
  [:aside.side-panel
   (panel-header
    {:title "Details"
     :subtitle "Dummy selection metadata"
     :actions (pane-toggle-button
               {:title "Collapse details"
                :icon :right
                :actions [[:layout/toggle-pane store :right]]})})
   (render-view views :details {})])

(defn search-pane [{:keys [store views]}]
  [:section.bottom-panel
   (panel-header
    {:title "Search"
     :subtitle "Dummy query workspace"
     :actions (pane-toggle-button
               {:title "Collapse search"
                :icon :down
                :actions [[:layout/toggle-pane store :bottom]]})})
   (render-view views :search {})])

(defn collapsed-details-rail [{:keys [store]}]
  [:button.collapsed-rail.collapsed-rail-vertical
   {:type "button"
    :title "Show details"
    :on {:click [[:layout/toggle-pane store :right]]}}
   [:span.collapsed-rail-icon (icons/chevron-icon :left)]
   [:span.collapsed-rail-text "Details"]])

(defn collapsed-search-rail [{:keys [store]}]
  [:button.collapsed-rail.collapsed-rail-horizontal
   {:type "button"
    :title "Show search"
    :on {:click [[:layout/toggle-pane store :bottom]]}}
   [:span.collapsed-rail-icon (icons/chevron-icon :up)]
   [:span.collapsed-rail-text "Search"]])

(defn body-layout [{:keys [store state views]}]
  (let [{:keys [layout]} state]
    [:div.workspace
     [:div.workspace-main
      [:div.center-stack
       {:id "replicant-center-stack"
        :style (center-stack-style layout)}
       [:div.center-row
        {:id "replicant-center-row"
         :style (center-row-style layout)}
        (main-panel {:store store :layout layout :views views})
        [:div.splitter.splitter-vertical
         {:on {:pointerdown [[:layout/begin-drag store :right]]}}]
        (if (:right-open? layout)
          (details-pane {:store store :views views})
          (collapsed-details-rail {:store store}))]
       [:div.splitter.splitter-horizontal
        {:on {:pointerdown [[:layout/begin-drag store :bottom]]}}]
       (if (:bottom-open? layout)
         (search-pane {:store store :views views})
         (collapsed-search-rail {:store store}))]]]))

(defn app-shell [{:keys [store state views]
                  :or {views dummy-content/default-views}}]
  (let [{:keys [layout]} state]
    [:div.app-shell
     {:style (root-style layout)}
     (navbar {:store store})
     (body-layout {:store store :state state :views views})]))
