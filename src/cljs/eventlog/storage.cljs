(ns eventlog.storage
  (:require
   [clojure.edn :as edn]
   [reagent.core :as r]))

(defprotocol ILayoutStore
  (load-layout! [this])
  (save-layout! [this])
  (reset-layout! [this])
  (layout-state [this])
  (set-layout! [this layout]))

(def default-layout
  {:main-view :list
   :right-open? true
   :bottom-open? true
   :right-collapsed-size 24
   :bottom-collapsed-size 28
   :right-size 320
   :bottom-size 220
   :right-last-size 320
   :bottom-last-size 220
   :split-chart-size 36})

(defrecord LocalStorageLayoutStore [storage-key state]
  ILayoutStore
  (load-layout! [this]
    (let [loaded
          (try
            (if-let [text (.getItem js/localStorage storage-key)]
              (merge default-layout (edn/read-string text))
              default-layout)
            (catch :default _
              default-layout))]
      (reset! state loaded)
      this))
  (save-layout! [this]
    (try
      (.setItem js/localStorage storage-key (pr-str @state))
      (catch :default _
        nil))
    this)
  (reset-layout! [this]
    (reset! state default-layout)
    (save-layout! this))
  (layout-state [_]
    state)
  (set-layout! [this layout]
    (reset! state layout)
    (save-layout! this)
    this))

(defn make-layout-store [storage-key]
  (->LocalStorageLayoutStore storage-key (r/atom default-layout)))

(defn swap-layout! [layout-store f & args]
  (let [state (layout-state layout-store)]
    (apply swap! state f args)
    (save-layout! layout-store)
    layout-store))
