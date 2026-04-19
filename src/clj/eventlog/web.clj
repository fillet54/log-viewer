(ns eventlog.web
  (:require
   [reitit.ring :as ring]
   [ring.adapter.jetty :as jetty]
   [ring.middleware.defaults :refer [site-defaults wrap-defaults]]
   [ring.util.response :as response]))

(defn index-handler [_request]
  (-> (response/resource-response "public/index.html")
      (response/content-type "text/html; charset=utf-8")))

(defn health-handler [_request]
  (-> (response/response "ok")
      (response/content-type "text/plain; charset=utf-8")))

(defn core-event-log-handler [{:keys [core-event-log]}]
  (-> (pr-str (:events core-event-log))
      response/response
      (response/content-type "application/edn; charset=utf-8")))

(defn app-handler [core-event-log]
  (wrap-defaults
   (ring/ring-handler
    (ring/router
     [["/" {:get index-handler}]
      ["/healthz" {:get health-handler}]
      ["/api/core-event/log" {:get (fn [_request]
                                     (core-event-log-handler {:core-event-log core-event-log}))}]])
    (ring/routes
     (ring/create-resource-handler {:path "/" :root "public"})
     (ring/create-default-handler)))
   (-> site-defaults
       (assoc-in [:security :anti-forgery] false))))

(defrecord WebServer [port core-event-log server]
  com.stuartsierra.component/Lifecycle
  (start [this]
    (if server
      this
      (assoc this
             :server (jetty/run-jetty (app-handler core-event-log)
                                      {:port port
                                       :join? false}))))
  (stop [this]
    (when server
      (.stop server))
    (assoc this :server nil)))

(defn new-web-server [{:keys [port]}]
  (map->WebServer {:port port}))
