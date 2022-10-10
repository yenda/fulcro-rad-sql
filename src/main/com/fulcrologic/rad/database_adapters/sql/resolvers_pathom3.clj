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

(defn id-resolver [{::attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               outputs (attr/attributes->eql attributes)
               schema  (::attr/schema id-attribute)]
    (let [transform (::pco/transform id-attribute)
          op-name (symbol
                   (str (namespace id-key))
                   (str (name id-key) "-resolver"))]
      (pco/resolver
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
                                 (group-by ::entity-id))]
    (log/info "Generating resolvers")
    (reduce-kv
     (fn [resolvers id-attr attributes]
       (log/info "Generating resolver for id key" (::attr/qualified-key id-attr)
                 "to resolve" (mapv ::attr/qualified-key attributes))
       (conj resolvers (id-resolver {::attr/id-attribute id-attr
                                     ::attr/attributes   attributes
                                     ::attr/k->attr      k->attr})))
     [] id-attr->attributes)))
