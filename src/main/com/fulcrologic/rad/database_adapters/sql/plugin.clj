(ns com.fulcrologic.rad.database-adapters.sql.plugin
  (:require
    [com.fulcrologic.fulcro.algorithms.do-not-use :refer [deep-merge]]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.database-adapters.sql :as sql]
    [com.fulcrologic.rad.database-adapters.sql.result-set :as sql.rs]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]))

(sql.rs/coerce-result-sets!)

(defn relationships [attributes]
  (let [k->attr             (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                                 (mapcat
                                  (fn [attribute]
                                    (for [entity-id (::attr/identities attribute)]
                                      (assoc attribute ::entity-id (k->attr entity-id)))))
                                 (group-by ::entity-id))]
    (->> attributes
         (filter #(= :one (::attr/cardinality %)))
         (mapcat
          (fn [attribute]
            (for [entity-id (::attr/identities attribute)]
              (let [target-entity (k->attr (::attr/target attribute))
                    target-entity-attributes  (id-attr->attributes target-entity)
                    target-attributes (filter #(and (::attr/cardinality %)
                                                    (contains? (::attr/identities attribute)
                                                               (::attr/target %)))
                                              target-entity-attributes)
                    _ (when (zero? (count target-attributes))
                        (throw (ex-info "Target attribute not found for"
                                        {:attribute attribute})))
                    _ (when (> (count target-attributes) 1)
                        (throw (ex-info "More than 1 target attribute not supported"
                                        {:attribute attribute
                                         :target-attributes target-attributes})))
                    target-attribute (first target-attributes)
                    relationship (if target-attribute
                                   (keyword (str (name (::attr/cardinality attribute))
                                                 "-to-"
                                                 (name (::attr/cardinality target-attribute))))
                                   :one-to-one)]
                (assoc attribute
                       ::entity-id (k->attr entity-id)
                       :target-attribute target-attribute
                       :relationship relationship)))))
         (remove #(= (:relationship %) :one-to-one))
         (enc/keys-by (comp ::attr/qualified-key :target-attribute)))))

(defn wrap-env
  "Env middleware to add the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
   `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  You should also pass the general config if possible, which should have an ::sql/databases key. This allows the
  correct vendor adapter to be selected for each database.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql.plugin/connection-pools`: The result of the database-mapper.
  "
  ([all-attributes database-mapper config] (wrap-env all-attributes nil database-mapper config))
  ([all-attributes base-wrapper database-mapper config]
   (let [database-configs (get config ::sql/databases)
         default-adapter  (vendor/->H2Adapter)
         vendor-adapters  (reduce-kv
                           (fn [acc k v]
                             (let [{:sql/keys [vendor schema]} v
                                   adapter (case vendor
                                             :postgresql (do
                                                           (log/info k "using PostgreSQL Adapter for schema" schema)
                                                           (vendor/->PostgreSQLAdapter))
                                             :h2 (do
                                                   (log/info k "using H2 Adapter for schema" schema)
                                                   (vendor/->H2Adapter))
                                             :mariadb (do
                                                        (log/info k "using MariaDB Adapter for schema" schema)
                                                        (vendor/->MariaDBAdapter))
                                             default-adapter)]
                               (assoc acc schema adapter)))
                           {}
                           database-configs)
         one-to-many-relationships (relationships all-attributes)]
     (fn [env]
       (cond-> (let [database-connection-map (database-mapper env)]
                 (assoc env
                        ::sql/one-to-many-relationships one-to-many-relationships
                        ::sql/default-adapter default-adapter
                        ::sql/adapters vendor-adapters
                        ::sql/connection-pools database-connection-map))
         base-wrapper (base-wrapper))))))

(defn pathom-plugin
  "A pathom 2 plugin that adds the necessary SQL connections and databases to the pathom env for
   a given request. Requires a database-mapper, which is a
  `(fn [pathom-env] {schema-name connection-pool})` for a given request.

  See also wrap-env.

  You should also pass the general config if possible, which should have an ::sql/databases key. This allows the
  correct vendor adapter to be selected for each database.

  The resulting pathom-env available to all resolvers will then have:

  - `::sql.plugin/connection-pools`: The result of the database-mapper.

  This plugin should run before (be listed after) most other plugins in the plugin chain since
  it adds connection details to the parsing env.
  "
  ([database-mapper]
   (pathom-plugin database-mapper {}))
  ([database-mapper config]
   (let [augment (wrap-env database-mapper config)]
     {:com.wsscode.pathom.core/wrap-parser
      (fn env-wrap-wrap-parser [parser]
        (fn env-wrap-wrap-internal [env tx]
          (parser (augment env) tx)))})))
