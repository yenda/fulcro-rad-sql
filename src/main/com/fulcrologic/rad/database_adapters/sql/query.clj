(ns com.fulcrologic.rad.database-adapters.sql.query
  "This namespace provides query builders for various concerns of
  a SQL database in a RAD application. The main concerns are:

  - Fetch queries (with joins) coming from resolvers
  - Building custom queries based of off RAD attributes
  - Persisting data based off submitted form deltas"
  (:require
   [com.fulcrologic.guardrails.core :refer [=> >defn ?]]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [jsonista.core :as j]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [taoensso.timbre :as log])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(defmacro timer
  [s expr params]
  `(let [start# (. System (nanoTime))
         _# (log/debug (format "Running %s" ~s) ~params)
         ret# ~expr
         duration# (/ (double (- (. System (nanoTime)) start#))
                      1000000.0)]
     (if (< 1000 duration#)
       (log/warn (format "Ran %s in %f msecs" ~s duration#) ~params)
       (log/debug (format "Ran %s in %f msecs" ~s duration#) ~params))
     ret#))

(defn RAD-column-reader
  "An example column-reader that still uses `.getObject` but expands CLOB
  columns into strings."
  [^ResultSet rs ^ResultSetMetaData md ^Integer i]
  (let [col-type (.getColumnType md i)
        col-type-name (.getColumnTypeName md i)]
    (cond
      (= col-type-name "JSON") (j/read-value (.getObject rs i))
      (= col-type Types/CLOB) (rs/clob->string (.getClob rs i))
      (#{Types/TIMESTAMP Types/TIMESTAMP_WITH_TIMEZONE} col-type) (.getTimestamp rs i)
      :else (.getObject rs i))))

;; Type coercion is handled by the row builder
(def row-builder (rs/as-maps-adapter rs/as-unqualified-kebab-maps RAD-column-reader))

(>defn eql-query!
       [{::rad.sql/keys [connection-pools]}
        query
        schema
        resolver-input]
       [any? vector? keyword? coll? => (? coll?)]
       (let [datasource        (or (get connection-pools schema) (throw (ex-info "Data source missing for schema" {:schema schema})))]
         (timer "SQL query with execute!"
                (jdbc/execute! datasource query {:builder-fn row-builder})
                {:query query})))
