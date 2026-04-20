(ns eventlog.sample.core-event
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]))

(def channel-labels ["A" "B" "C" "D"])

(defn resolve-resource-path [resource-path]
  (or (io/resource resource-path)
      (let [file (io/file "resources" resource-path)]
        (when (.exists file)
          file))))

(defn load-edn-resource [resource-path]
  (if-let [resource (resolve-resource-path resource-path)]
    (-> resource
        slurp
        edn/read-string)
    (throw (ex-info "EDN resource not found"
                    {:resource-path resource-path}))))

(defn any-all-label [value name]
  (if (zero? value)
    [0 "Any" "All"]
    [value name]))

(defn resolve-subsystem-entry [subsystem-map system-id subsystem-id unit-id]
  (let [systems (:systems subsystem-map)
        system-entry (get systems system-id (get systems 0))
        subsystem-entry (get-in system-entry [:subsystems subsystem-id]
                                (get-in system-entry [:subsystems 0]))
        unit-name (get-in subsystem-entry [:units unit-id]
                          (get-in subsystem-entry [:units 0] "Unknown"))]
    {:system (any-all-label system-id (:name system-entry))
     :subsystem (any-all-label subsystem-id (:name subsystem-entry))
     :unit (any-all-label unit-id unit-name)}))

(defn channel-string [channel-count]
  (apply str (take channel-count channel-labels)))

(defn choose-channel-group [channel-count]
  (let [all-channels (vec (take channel-count channel-labels))]
    (if (or (= channel-count 1) (< (rand) 0.75))
      (apply str all-channels)
      (let [selected (->> all-channels
                          (filter (fn [_] (< (rand) 0.82)))
                          vec)
            selected (if (seq selected)
                       selected
                       [(rand-nth all-channels)])]
        (apply str selected)))))

(defn random-unit-id [event]
  (if (<= (:units event) 1)
    1
    (inc (rand-int (:units event)))))

(defn fault-key [event unit-id]
  [(:system event) (:subsystem event) unit-id (:code event)])

(defn seconds->iso8601 [epoch-seconds]
  (.format
   (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss'Z'")
   (.atOffset (java.time.Instant/ofEpochSecond epoch-seconds) java.time.ZoneOffset/UTC)))

(defn event-data [event unit-id is-set]
  {:source "sample-generator"
   :set-state (if is-set "SET" "CLEAR")
   :observed-unit unit-id
   :limit (:limit event)
   :note (str (str/replace (:name event) "_" " ")
              (if is-set " asserted" " cleared"))})

(defn build-output-event [event subsystem-map unit-id utc-seconds boot-seconds channel-count is-set]
  (let [{:keys [system subsystem unit]} (resolve-subsystem-entry subsystem-map (:system event) (:subsystem event) unit-id)
        channels (choose-channel-group channel-count)
        utc-time (seconds->iso8601 utc-seconds)]
    {:time [utc-time boot-seconds]
     :type "core-event"
     :num-channels channel-count
     :channels channels
     :name (:name event)
     :system system
     :subsystem subsystem
     :unit unit
     :code (:code event)
     :severity (:severity event)
     :is_set is-set
     :description (:description event)
     :data (event-data event unit-id is-set)}))

(defn candidate-set-faults [events active-faults]
  (reduce
   (fn [acc event]
     (into acc
           (for [unit-id (range 1 (inc (max 1 (:units event))))
                 :let [key (fault-key event unit-id)]
                 :when (not (contains? active-faults key))]
             {:event event
              :unit-id unit-id
              :key key})))
   []
   events))

(defn choose-action [active-faults]
  (cond
    (empty? active-faults) :set
    (< (rand) 0.58) :set
    :else :clear))

(defn choose-clear-candidate [active-faults current-time]
  (let [active-values (vals active-faults)
        eligible (->> active-values
                      (filter (fn [{:keys [set-time]}]
                                (> (- current-time set-time) (+ 15 (rand-int 240)))))
                      vec)]
    (if (seq eligible)
      (rand-nth eligible)
      (rand-nth (vec active-values)))))

(defn next-time-step [events-per-hour]
  (let [mean-gap (/ 3600.0 (max 1 events-per-hour))
        jitter (+ 0.2 (* 1.6 (rand)))]
    (max 1 (long (Math/round (* mean-gap jitter))))))

(defn target-event-count [hours]
  (let [rate-per-hour (+ 60 (rand-int 360))]
    (* hours rate-per-hour)))

(defn generate-events
  [{:keys [event-list subsystem-map channels hours start-utctime]
    :or {channels 4
         hours 1
         start-utctime 1735689600}}]
  (let [max-channels (min 4 (max 1 channels))
        total-target (target-event-count hours)
        end-time (* hours 3600)]
    (loop [boot-seconds 0
           utc-seconds start-utctime
           active-faults {}
           output []]
      (if (or (>= boot-seconds end-time)
              (>= (count output) total-target))
        output
        (let [action (choose-action active-faults)
              next-output
              (case action
                :clear
                (let [{:keys [event unit-id key]} (choose-clear-candidate active-faults boot-seconds)
                      line (build-output-event event subsystem-map unit-id utc-seconds boot-seconds max-channels false)]
                  {:line line
                   :active-faults (dissoc active-faults key)})
                :set
                (let [candidates (candidate-set-faults event-list active-faults)]
                  (if (seq candidates)
                    (let [{:keys [event unit-id key]} (rand-nth candidates)
                          line (build-output-event event subsystem-map unit-id utc-seconds boot-seconds max-channels true)]
                      {:line line
                       :active-faults (assoc active-faults key {:event event
                                                                :unit-id unit-id
                                                                :key key
                                                                :set-time boot-seconds})})
                    nil)))
              step (next-time-step (max 1 (/ total-target hours)))]
          (if next-output
            (recur (+ boot-seconds step)
                   (+ utc-seconds step)
                   (:active-faults next-output)
                   (conj output (:line next-output)))
            (recur (+ boot-seconds step)
                   (+ utc-seconds step)
                   active-faults
                   output)))))))

(defn parse-long-arg [value default]
  (try
    (Long/parseLong (str value))
    (catch Exception _
      default)))

(defn -main [& args]
  (let [[hours-arg channels-arg] args
        hours (parse-long-arg hours-arg 1)
        channels (parse-long-arg channels-arg 4)
        event-list (load-edn-resource "data/core_event/core_event_list.edn")
        subsystem-map (load-edn-resource "data/core_event/subsystem.edn")
        events (generate-events {:event-list event-list
                                 :subsystem-map subsystem-map
                                 :channels channels
                                 :hours hours})]
    (pprint/pprint events)))
