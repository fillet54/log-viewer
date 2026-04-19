(ns eventlog.main
  (:gen-class)
  (:require
   [com.stuartsierra.component :as component]
   [eventlog.system :as system]))

(defonce running-system (atom nil))

(defn start! []
  (let [app (component/start-system (system/new-system {:port 3000}))]
    (reset! running-system app)
    (println "Server started on http://localhost:3000")))

(defn stop! []
  (when-let [app @running-system]
    (component/stop-system app)
    (reset! running-system nil)
    (println "Server stopped")))

(defn -main [& _args]
  (start!))
