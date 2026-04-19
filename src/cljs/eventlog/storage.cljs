(ns eventlog.storage
  (:require
   [clojure.edn :as edn]))

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
   :split-chart-size 220})

(defn normalize-layout [layout]
  (update layout :split-chart-size
          (fn [value]
            (let [value (or value (:split-chart-size default-layout))]
              (if (<= value 100)
                (:split-chart-size default-layout)
                value)))))

(defn load-layout [storage-key]
  (try
    (if-let [text (.getItem js/localStorage storage-key)]
      (-> (merge default-layout (edn/read-string text))
          normalize-layout)
      default-layout)
    (catch :default _
      default-layout)))

(defn save-layout! [storage-key layout]
  (try
    (.setItem js/localStorage storage-key (pr-str layout))
    (catch :default _
      nil))
  layout)

(defn reset-layout! [storage-key]
  (save-layout! storage-key default-layout))
