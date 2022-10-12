(ns com.fulcrologic.rad.database-adapters.sql.resolvers-pathom3
  (:require
    [clojure.pprint :refer [pprint]]
    [com.fulcrologic.rad.authorization :as auth]
    [com.fulcrologic.rad.attributes :as attr]
    [com.fulcrologic.rad.form :as rad.form]
    [com.fulcrologic.rad.options-util :refer [?!]]
    [com.fulcrologic.guardrails.core :refer [>defn => |]]
    [com.fulcrologic.rad.database-adapters.sql :as rad.sql]
    [com.fulcrologic.rad.database-adapters.sql.query :as sql.query]
    [com.fulcrologic.rad.database-adapters.sql.schema :as sql.schema]
    [taoensso.encore :as enc]
    [taoensso.timbre :as log]
    [com.wsscode.pathom3.connect.operation :as pco]
    [next.jdbc.sql :as jdbc.sql]
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]

    ;; IMPORTANT: This turns on instant coercion:
    [next.jdbc.date-time]

    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.ids :as ids]
    [clojure.string :as str]
    [clojure.set :as set]
    [edn-query-language.core :as eql]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]))

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::attr/keys [id-attribute key->attribute] :as env} input]
  (enc/if-let [query* (get env ::rad.sql/default-query)]
    (let [result (sql.query/eql-query! env id-attribute query* input)]
      result)
    (do
      (log/info "Unable to complete query.")
      nil)))

(defn entity-by-ref-query
  "The entity query used by the pathom resolvers."
  [{::attr/keys [id-attribute ref-attribute key->attribute] :as env} input]
  (enc/if-let [query* (get env ::rad.sql/default-query)]
    (let [result (sql.query/eql-query-by-ref! env id-attribute ref-attribute query* input)]
      result)
    (do
      (log/info "Unable to complete query.")
      nil)))

(defn id-resolver [{::attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               outputs (conj (attr/attributes->eql attributes) id-key)
               schema  (::attr/schema id-attribute)]
    (let [transform (::pco/transform id-attribute)
          op-name (symbol
                   (str (namespace id-key))
                   (str (name id-key) "-resolver"))]
      (pco/resolver op-name
                    (cond-> {::pco/output  outputs
                             ::pco/batch?  true
                             ::pco/resolve (fn [env input]
                                             (auth/redact env
                                                          (log/spy :trace (entity-query
                                                                           (assoc env
                                                                                  ::attr/id-attribute id-attribute
                                                                                  ::attr/schema schema
                                                                                  ::rad.sql/default-query outputs)
                                                                           (log/spy :trace input)))))
                             ::pco/input [id-key]}
                      transform (assoc ::pco/transform transform))))
    (log/error
     "Unable to generate id-resolver. Attribute was missing schema, "
     "or could not be found" (::attr/qualified-key id-attribute))))

(defn ref-resolver [{::attr/keys [id-attribute attributes ref-attribute k->attr]}]
  (enc/if-let [id-key (::attr/qualified-key id-attribute)
               ref-attribute-key (::attr/qualified-key ref-attribute)
               ref-attribute-target (::attr/target ref-attribute)
               outputs (conj (attr/attributes->eql attributes) id-key)
               schema  (::attr/schema id-attribute)]
    (let [transform (::pco/transform id-attribute)
          op-name (symbol
                   (str (namespace id-key))
                   (str "one-by-" (str (str/replace (namespace ref-attribute-target) #"\." "-")) "-" (name ref-attribute-target) "-resolver"))]
      (pco/resolver op-name
                    (cond-> {::pco/output  outputs
                             ::pco/batch?  true
                             ::pco/resolve (fn [env input]
                                             (auth/redact env
                                                          (log/spy :trace (entity-by-ref-query
                                                                           (assoc env
                                                                                  ::attr/id-attribute id-attribute
                                                                                  ::attr/ref-attribute ref-attribute
                                                                                  ::attr/schema schema
                                                                                  ::rad.sql/default-query outputs)
                                                                           (log/spy :trace input)))))
                             ::pco/input [ref-attribute-target]}
                      transform (assoc ::pco/transform transform))))
    (log/error
     "Unable to generate join-by-id-resolver. Attribute was missing schema, "
     "or could not be found" (::attr/qualified-key id-attribute))))

(defn many-ref-resolver [{::attr/keys [id-attribute attributes ref-attribute k->attr id-attr->attributes]}]
  (enc/if-let [id-attr-key->attributes (enc/map-keys ::attr/qualified-key id-attr->attributes)
               attr-key->attribute (enc/map-keys ::attr/qualified-key attributes)
               id-key (::attr/qualified-key id-attribute)
               ref-attribute-key (::attr/qualified-key ref-attribute)
               ref-attribute-target (::attr/target ref-attribute)
               outputs [{ref-attribute-key (conj (mapv ::attr/qualified-key (id-attr-key->attributes ref-attribute-target)) id-key)}]
               schema  (::attr/schema id-attribute)]
    (let [transform (::pco/transform id-attribute)
          op-name (symbol
                   (str (namespace ref-attribute-key))
                   (str (name ref-attribute-key) "-resolver"))]
      (pco/resolver op-name
                    (cond-> {::pco/output  outputs
                             ::pco/batch?  true
                             ::pco/resolve (fn [env input]
                                             (auth/redact env
                                                          (log/spy :trace (entity-by-ref-query
                                                                           (assoc env
                                                                                  ::attr/id-attribute id-attribute
                                                                                  ::attr/attributes (id-attr-key->attributes ref-attribute-target)
                                                                                  ::attr/ref-attribute (assoc ref-attribute ::rad.sql/column-name (::rad.sql/column-name (k->attr  (::rad.sql/ref ref-attribute))))
                                                                                  ::attr/target-attribute (k->attr ref-attribute-target)
                                                                                  ::attr/schema schema

                                                                                  ::rad.sql/default-query outputs)
                                                                           (log/spy :trace input)))))
                             ::pco/input [id-key]}
                      transform (assoc ::pco/transform transform))))
    (log/error
     "Unable to generate join-by-id-resolver. Attribute was missing schema, "
     "or could not be found" (::attr/qualified-key id-attribute))))

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
        _ (log/info "Generating resolvers")
        id-resolvers
        (reduce-kv
         (fn [resolvers id-attr attributes]
           (log/info "Generating resolver for id key" (::attr/qualified-key id-attr)
                     "to resolve" (mapv ::attr/qualified-key attributes))
           (conj resolvers (id-resolver {::attr/id-attribute id-attr
                                         ::attr/attributes   attributes
                                         ::attr/k->attr      k->attr})))
         [] id-attr->attributes)
        id-attr->ref-attributes
        (->> attributes
             (filter #(= schema (::attr/schema %)))
             (filter #(#{:one} (::attr/cardinality %)))
             (mapcat
              (fn [attribute]
                (for [entity-id (::attr/identities attribute)]
                  (assoc attribute ::entity-id (k->attr entity-id)))))
             (group-by ::entity-id))
        #_#_ref-resolvers
        (reduce-kv
         (fn [resolvers id-attr attributes]
           (reduce (fn [resolvers ref-attribute]
                     (log/info "Generating ref resolver for one" (::attr/qualified-key ref-attribute)
                               "for id key" (::attr/qualified-key id-attr))
                     (conj resolvers (ref-resolver {::attr/id-attribute id-attr
                                                    ::attr/ref-attribute ref-attribute
                                                    ::attr/attributes   attributes
                                                    ::attr/k->attr      k->attr})))
                   resolvers
                   attributes))
         []
         id-attr->ref-attributes)

        id-attr->many-ref-attributes
        (->> attributes
             (filter #(= schema (::attr/schema %)))
             (filter #(#{:many} (::attr/cardinality %)))
             (mapcat
              (fn [attribute]
                (for [entity-id (::attr/identities attribute)]
                  (assoc attribute ::entity-id (k->attr entity-id)))))
             (group-by ::entity-id))

        many-ref-resolvers
        (reduce-kv
         (fn [resolvers id-attr ref-attributes]
           (reduce (fn [resolvers ref-attribute]
                     (log/info "Generating resolver for many" (::attr/qualified-key ref-attribute)
                               "for id key" (::attr/qualified-key id-attr))
                     (conj resolvers (many-ref-resolver {::attr/id-attribute id-attr
                                                         ::attr/id-attr->attributes id-attr->attributes
                                                         ::attr/ref-attribute ref-attribute
                                                         ::attr/ref-attributes   ref-attributes
                                                         ::attr/attributes attributes
                                                         ::attr/k->attr      k->attr})))
                   resolvers
                   ref-attributes))
         []
         id-attr->many-ref-attributes)]
    (vec (concat id-resolvers #_ref-resolvers many-ref-resolvers))))
