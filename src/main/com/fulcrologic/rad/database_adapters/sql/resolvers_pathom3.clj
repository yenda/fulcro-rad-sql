(ns com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [com.fulcrologic.rad.attributes :as attr]
   [com.fulcrologic.rad.authorization :as auth]
   [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
   [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
   [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
   [com.wsscode.pathom3.connect.operation :as pco]
   [honey.sql :as sql]
   [taoensso.encore :as enc]
   [taoensso.timbre :as log]))

(defn get-column [attribute]
  (or (::rad.sql/column-name attribute)
      (-> (::attr/qualified-key attribute) name keyword)))

(defn get-table [attribute]
  (::rad.sql/table attribute))

(defn get-outputs
  [id-attribute-key id-attribute->attributes k->attr]
  (let [id-attribute (k->attr id-attribute-key)
        outputs (reduce (fn [acc attr]
                          (if (= :many (::attr/cardinality attr))
                            acc
                            (conj acc attr)))
                        [id-attribute]
                        (id-attribute->attributes id-attribute))]
    outputs))


(defn id-resolver [{::attr/keys [id-attribute id-attr->attributes attributes k->attr] :as c}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               output-attributes (get-outputs id-key id-attr->attributes k->attr)
               outputs (mapv ::attr/qualified-key output-attributes)
               output-column->output-key (reduce (fn [acc output]
                                                   (assoc acc (get-column output) (::attr/qualified-key output)))
                                                 {}
                                                 output-attributes)
               schema  (::attr/schema id-attribute)]
    (let [transform (::pco/transform id-attribute)
          op-name (symbol
                   (str (namespace id-key))
                   (str (name id-key) "-resolver"))
          id-resolver (pco/resolver op-name
                                    (cond-> {::pco/output outputs
                                             ::pco/batch?  true
                                             ::pco/resolve (fn [env input]

                                                             (let [ids (mapv id-key input)
                                                                   query (sql/format {:select (mapv get-column
                                                                                                    output-attributes)
                                                                                      :from (get-table id-attribute)
                                                                                      :where [:in (get-column id-attribute) ids]})
                                                                   #_ (log/info :SQL query)
                                                                   rows (sql.query/eql-query! env
                                                                                              query
                                                                                              schema
                                                                                              input)
                                                                   results-by-id (reduce (fn [acc row]
                                                                                           (let [result (clojure.set/rename-keys row output-column->output-key)]
                                                                                             (assoc acc
                                                                                                    {id-key (id-key result)}
                                                                                                    result)))
                                                                                         {}
                                                                                         rows)
                                                                   results (mapv (fn [result]
                                                                                   (get results-by-id {id-key result})) ids)]
                                                               (auth/redact env
                                                                            results)))
                                             ::pco/input [id-key]}
                                      transform (assoc ::pco/transform transform)))]
      [id-resolver])
    (log/error
     "Unable to generate id-resolver. Attribute was missing schema, "
     "or could not be found" (::attr/qualified-key id-attribute))))

(defn one-to-one-relationship-resolver [{::attr/keys [qualified-key target schema]
                                         ::rad.sql/keys [column-name]
                                         :keys [target-attribute outputs]
                                         :as relationship-attribute}
                                        attributes
                                        k->attr
                                        id-attr->attributes]
  (if-not target-attribute
    (throw (ex-info "resolution of one-to-one relationship requires a target attribute"
                    {:relationship-attribute relationship-attribute}))
    (let [;; NOTE: there is no need for an alias the other side of the relationship
          ;; will create a resolver
          _ (log/debug "Building Pathom3 resolver for" qualified-key "by" target)
          transform (::pco/transform relationship-attribute)
          op-name (symbol
                   (str (namespace qualified-key))
                   (str "-by-" (str (str/replace (namespace target) #"\." "-")) "-" (name target) "-resolver"))
          id-attribute (::entity-id relationship-attribute)
          id-key  (::attr/qualified-key id-attribute)
          output-attributes (get-outputs id-key id-attr->attributes k->attr)
          outputs (mapv ::attr/qualified-key output-attributes)
          output-column->output-key (reduce (fn [acc output]
                                              (assoc acc (get-column output) (::attr/qualified-key output)))
                                            {}
                                            output-attributes)
          schema  (::attr/schema relationship-attribute)
          entity-by-attribute-resolver
          (pco/resolver op-name
                        (cond-> {::pco/output  outputs
                                 ::pco/batch?  true
                                 ::pco/resolve (fn [env input]
                                                 (let [ids (mapv target input)
                                                       query (sql/format {:select (mapv get-column
                                                                                        output-attributes)
                                                                          :from (get-table id-attribute)
                                                                          :where [:in (get-column relationship-attribute) ids]})
                                                       #_ (log/info :SQL query)
                                                       rows (sql.query/eql-query! env
                                                                                  query
                                                                                  schema
                                                                                  input)
                                                       results-by-id (reduce (fn [acc row]
                                                                               (let [result (clojure.set/rename-keys row output-column->output-key)]
                                                                                 (assoc acc
                                                                                        {target (qualified-key result)}
                                                                                        result)))
                                                                             {}
                                                                             rows)
                                                       results (mapv #(get results-by-id {target %}) ids)]
                                                   (auth/redact env
                                                                results)))
                                 ::pco/input [target]}
                          transform (assoc ::pco/transform transform)))]
      [entity-by-attribute-resolver])))

(defn one-to-many-relationship-resolver
  [{::attr/keys [qualified-key target schema]
    ::rad.sql/keys [column-name]
    :keys [target-attribute outputs]
    :as relationship-attribute}
   attributes
   k->attr
   id-attr->attributes]
  (if-not target-attribute
    (do
      (throw (ex-info "resolution of one-to-many relationship requires a target attribute"
                      {:relationship-attribute relationship-attribute})))
    (let [_ (log/debug "Building Pathom3 alias for one-to-many resolver from" qualified-key "to" target)
          alias-resolver (pbir/alias-resolver qualified-key target)
          target-key (::attr/qualified-key target-attribute)
          transform (::pco/transform relationship-attribute)
          op-name (symbol
                   (str (namespace target-key))
                   (str (name target-key) "-resolver"))
          _ (log/debug "Building Pathom3 resolver" op-name "for" qualified-key "by" target)
          id-attribute (::entity-id relationship-attribute)
          id-key  (::attr/qualified-key id-attribute)
          output-attributes (get-outputs id-key id-attr->attributes k->attr)
          outputs (mapv ::attr/qualified-key output-attributes)
          output-column->output-key (reduce (fn [acc output]
                                              (assoc acc (get-column output) (::attr/qualified-key output)))
                                            {}
                                            output-attributes)
          schema  (::attr/schema relationship-attribute)
          order-by (some-> (::rad.sql/order-by target-attribute)
                           k->attr
                           get-column)
          entity-by-attribute-resolver
          (pco/resolver
           op-name
           (cond-> {::pco/output  [{target-key [id-key]}]
                    ::pco/batch?  true
                    ::pco/resolve
                    (fn [env input]
                      (let [ids (mapv target input)
                            relationship-column (get-column relationship-attribute)
                            query (sql/format
                                   {:select [relationship-column [[:json_arrayagg (if order-by
                                                                                    [:order-by (get-column id-attribute) order-by]
                                                                                    (get-column id-attribute))] :k]]
                                    :from (get-table id-attribute)
                                    :where [:in relationship-column ids]
                                    :group-by [relationship-column]})
                            _ (log/info :SQL query {:select [relationship-column [[:json_arrayagg (get-column id-attribute)] :k]]
                                                    :from (get-table id-attribute)
                                                    :where [:in relationship-column ids]
                                                    :group-by [relationship-column]})
                            rows (sql.query/eql-query! env
                                                       query
                                                       schema
                                                       input)
                            results-by-id (reduce (fn [acc row]
                                                    (let [result (clojure.set/rename-keys row output-column->output-key)]
                                                      (assoc acc
                                                             {target (qualified-key result)}
                                                             {target-key (mapv (fn [k]
                                                                                 {id-key k}) (:k result))})))
                                                  {}
                                                  rows)
                            results (mapv #(get results-by-id {target %}) ids)]

                        (auth/redact env results)))
                    ::pco/input [target]}
             transform (assoc ::pco/transform transform)))]
      [alias-resolver entity-by-attribute-resolver])))



(defn relationships [attributes schema k->attr id-attribute->attributes]
  (let [{:keys [one-to-one one-to-many]}
        (->> attributes
             (filter #(= schema (::attr/schema %)))
             (filter #(= :one (::attr/cardinality %)))
             (mapcat
              (fn [attribute]
                (for [entity-id (::attr/identities attribute)]
                  (let [target-entity (k->attr (::attr/target attribute))
                        target-entity-attributes  (id-attribute->attributes target-entity)
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
                           :outputs (get-outputs entity-id id-attribute->attributes k->attr)
                           :target-attribute target-attribute
                           :relationship relationship)))))
             (group-by :relationship))]
    [one-to-one one-to-many]))

(defn generate-resolvers
  "Returns a sequence of resolvers that can resolve attributes from
  SQL databases."
  [attributes schema]
  (log/info "Generating resolvers for SQL schema" schema)
  (let [k->attr             (enc/keys-by ::attr/qualified-key attributes)
        id-attr->attributes (->> attributes
                                 (filter #(= schema (::attr/schema %)))
                                 (mapcat
                                  (fn [attribute]
                                    (for [entity-id (::attr/identities attribute)]
                                      (assoc attribute ::entity-id (k->attr entity-id)))))
                                 (group-by ::entity-id))
        _ (log/debug "Generating resolvers")
        id-resolvers
        (reduce-kv
         (fn [resolvers id-attr attributes]
           (log/debug "Generating resolver for id key" (::attr/qualified-key id-attr)
                      "to resolve" (mapv ::attr/qualified-key attributes))
           (concat resolvers (id-resolver {::attr/id-attribute id-attr
                                           ::attr/id-attr->attributes id-attr->attributes
                                           ::attr/attributes   attributes
                                           ::attr/k->attr      k->attr})))
         [] id-attr->attributes)

        [one-to-one-relationships one-to-many-relationships] (relationships attributes schema k->attr id-attr->attributes)
        one-to-one-resolvers
        (reduce
         (fn [resolvers relationship]

           (log/debug "Generating resolvers for one to one relationship between"
                      (-> relationship ::entity-id ::attr/qualified-key)
                      "and" (::attr/target relationship))
           (concat resolvers (one-to-one-relationship-resolver relationship
                                                               attributes
                                                               k->attr
                                                               id-attr->attributes)))
         []
         one-to-one-relationships)

        one-to-many-resolvers
        (reduce
         (fn [resolvers relationship]

           (log/debug "Generating resolvers for one to many relationship between"
                      (-> relationship ::entity-id ::attr/qualified-key)
                      "and" (::attr/target relationship))
           (concat resolvers (one-to-many-relationship-resolver relationship
                                                                attributes
                                                                k->attr
                                                                id-attr->attributes)))
         []
         one-to-many-relationships)

        ]
    (vec (concat id-resolvers one-to-one-resolvers one-to-many-resolvers))))
