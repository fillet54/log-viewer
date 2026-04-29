(ns eventlog.views.eventviewer.navbar
  (:require
   [eventlog.views.eventviewer.icons :as icons :refer [icon]]))


(defn pill [{:keys [icon-label text]}]
  [:div {:class "nav-pill"}
   (when icon-label
     (icon icon-label))
   [:span {:class "nav-pill-text"} text]])

(defn profile-menu [{:keys [name email initials avatar-url]}]
  [:button
   {:class "nav-profile-menu"
    :aria-label "Open user menu"
    :type "button"}
   (if avatar-url
     [:img {:class "nav-profile-avatar"
            :src avatar-url
            :alt (or name "User avatar")}]
     [:div {:class "nav-profile-initials"}
      (or initials "U")])
   [:div {:class "nav-profile-copy"}
    [:div {:class "nav-profile-name"}
     (or name "User")]
    (when email
      [:div {:class "nav-profile-email"}
       email])]
   (icon :chevron-down)])

(defn navbar
  [state]
  (let [title         (:app/title state "Log Viewer")
        home-href     (:nav/home-href state "/")
        source        (:log/source state)
        start-date    (:log/start-date state)
        end-date      (:log/end-date state)
        row-count     (:log/row-count state)
        level-summary (:log/level-summary state)
        date-range    (cond
                        (and start-date end-date) (str start-date " → " end-date)
                        start-date start-date
                        end-date end-date
                        :else nil)
        user          {:name       (:user/name state)
                       :email      (:user/email state)
                       :initials   (:user/initials state)
                       :avatar-url (:user/avatar-url state)}]
    [:nav {:class "nav-root"}

     ;; Left: home + page/source heading
     [:div {:class "nav-left"}
      [:a {:class "nav-home-button"
           :href home-href
           :aria-label "Home"}
       (icon :home)]
      [:div {:class "nav-title-block"}
       [:div {:class "nav-title"} title]
       (when source
         [:div {:class "nav-subtitle"} source])]]

     ;; Center: compact data overview
     [:div {:class "nav-data-overview"}
      (when date-range
        (pill {:icon-label :calendar :text date-range}))
      (when row-count
        (pill {:icon-label :database :text row-count}))
      (when level-summary
        (pill {:icon-label :activity :text level-summary}))]

     ;; Right: profile/user menu
     [:div {:class "nav-right"}
      (profile-menu user)]]))
