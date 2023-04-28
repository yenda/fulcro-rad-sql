(ns com.fulcrologic.rad.database-adapters.sql.middleware
  (:require
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [com.fulcrologic.rad.form :as form]
    [com.fulcrologic.rad.database-adapters.sql.resolvers :as sql.resolvers]))

(defn wrap-sql-save
  "Form save middleware to accomplish SQL saves."
  ([]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result (sql.resolvers/save-form! pathom-env params)]
       save-result)))
  ([handler]
   (fn [{::form/keys [params] :as pathom-env}]
     (let [save-result    (sql.resolvers/save-form! pathom-env params)
           handler-result (handler pathom-env)]
       (deep-merge save-result handler-result)))))
