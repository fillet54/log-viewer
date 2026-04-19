(ns eventlog.views.eventviewer.data)

(defn branch-paths
  ([value]
   (branch-paths [] value))
  ([path value]
   (cond
     (map? value)
     (let [entries (seq value)]
       (into (if entries [path] [])
             (mapcat (fn [[key child]]
                       (branch-paths (conj path key) child)))
             entries))

     (sequential? value)
     (let [entries (seq value)]
       (into (if entries [path] [])
             (mapcat (fn [[idx child]]
                       (branch-paths (conj path idx) child)))
             (map-indexed vector value)))

     :else
     [])))

(defn expanded-paths-for [event]
  (into #{}
        (branch-paths (:data event))))
