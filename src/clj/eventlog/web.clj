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

(defn app-handler []
  (wrap-defaults
   (ring/ring-handler
    (ring/router
     [["/" {:get index-handler}]
      ["/healthz" {:get health-handler}]])
    (ring/routes
     (ring/create-resource-handler {:path "/" :root "public"})
     (ring/create-default-handler)))
   (-> site-defaults
       (assoc-in [:security :anti-forgery] false))))

(defrecord WebServer [port server]
  com.stuartsierra.component/Lifecycle
  (start [this]
    (if server
      this
      (assoc this
             :server (jetty/run-jetty (app-handler)
                                      {:port port
                                       :join? false}))))
  (stop [this]
    (when server
      (.stop server))
    (assoc this :server nil)))

(defn new-web-server [{:keys [port]}]
  (map->WebServer {:port port}))
