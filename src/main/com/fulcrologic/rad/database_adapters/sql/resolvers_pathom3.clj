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
      (:column attribute)
      (some-> (::attr/qualified-key attribute) name keyword)
      (throw (ex-info "Can't find column name for attribute" {:attribute attribute}))))

(defn get-table [attribute]
  (::rad.sql/table attribute))

(defn to-one-keyword [qualified-key]
  (keyword (str (namespace qualified-key)
                "/"
                (name qualified-key)
                "-id")))

(defn get-column->outputs
  [id-attr-k id-attr->attributes k->attr target?]
  (let [{::attr/keys [cardinality qualified-key target] :as id-attribute} (k->attr id-attr-k)
        outputs (reduce (fn [acc {::attr/keys [cardinality qualified-key target]
                                  ::rad.sql/keys [ref] :as attr}]
                          (if (or (= :many cardinality)
                                  (and (= :one cardinality) (and ref (not target?))))
                            acc
                            (let [column (get-column attr)
                                  outputs (if cardinality
                                            [(to-one-keyword qualified-key)
                                             {qualified-key [target]}]
                                            [qualified-key])]
                              (assoc acc column outputs))))
                        {(get-column id-attribute) [qualified-key]}
                        (id-attr->attributes id-attribute))]
    outputs))

(defn get-column-mapping [column->outputs]
  (reduce-kv (fn [acc column outputs]
               (reduce (fn [acc output]
                         (if (keyword? output)
                           (conj acc [[output] column])
                           (conj acc [(vec (flatten (vec output))) column])))
                       acc
                       outputs))
             []
             column->outputs))

(defn id-resolver [{::attr/keys [id-attr id-attr->attributes attributes k->attr] :as c}]
  (let [id-attr-k  (::attr/qualified-key id-attr)
        column->outputs (get-column->outputs id-attr-k id-attr->attributes k->attr false)
        column-mapping (get-column-mapping column->outputs)
        outputs (vec (flatten (vals column->outputs)))
        columns (keys column->outputs)
        table (get-table id-attr)
        id-column (get-column id-attr)]
    (log/debug "Generating resolver for id key" id-attr-k
               "to resolve" outputs)
    (let [{::attr/keys [schema]
           ::pco/keys [transform]} id-attr
          op-name (symbol
                   (str (namespace id-attr-k))
                   (str (name id-attr-k) "-resolver"))
          id-resolver
          (pco/resolver
           op-name
           (cond->
               {::pco/output outputs
                ::pco/batch?  true
                ::pco/resolve
                (fn [env input]

                  (let [ids (mapv id-attr-k input)
                        query (sql/format {:select columns
                                           :from table
                                           :where [:in id-column ids]})
                        rows (sql.query/eql-query! env
                                                   query
                                                   schema
                                                   input)
                        results-by-id (reduce
                                       (fn [acc row]
                                         (let [result
                                               (reduce
                                                (fn [acc [output-path column]]
                                                  (let [value (get row column)]
                                                    ;; if ref we don't return it
                                                    (if (and (nil? value) (= (count output-path) 2))
                                                      acc
                                                      (assoc-in acc output-path value))))
                                                {}
                                                column-mapping)]
                                           (assoc acc
                                                  (id-attr-k result)
                                                  result)))
                                       {}
                                       rows)
                        results (mapv (fn [id]
                                        (get results-by-id id)) ids)]
                    (auth/redact env
                                 results)))
                ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      id-resolver)))

(defn to-many-resolvers
  [{::attr/keys [schema]
    ::rad.sql/keys [ref] ;; user/organization
    ::pco/keys [transform] :as attr
    attr-k ::attr/qualified-key ;; organization/users
    target-k ::attr/target} ;; user/id
   id-attr-k ;; organization/id
   id-attr->attributes
   k->attr]
  (when-not ref
    (throw (ex-info "Missing ref in to-many ref" attr)))
  (let [op-name (symbol
                 (str (namespace attr-k))
                 (str (name attr-k) "-resolver"))
        _ (log/debug "Building Pathom3 resolver" op-name "for" attr-k "by" id-attr-k)
        target-attr (or (k->attr target-k)
                        (throw (ex-info "Target attribute not found" attr)))
        table (get-table target-attr)
        ref-attr (or (k->attr ref)
                     (throw (ex-info "Ref attribute not found" attr)))
        relationship-column (get-column ref-attr)
        target-column (get-column target-attr)
        order-by (some-> (::rad.sql/order-by attr)
                         k->attr
                         get-column)
        select [[relationship-column :k] [[:array_agg (if order-by
                                                        [:order-by target-column order-by]
                                                        target-column)] :v]]
        entity-by-attribute-resolver
        (pco/resolver
         op-name
         (cond-> {::pco/output  [{attr-k [target-k]}]
                  ::pco/batch?  true
                  ::pco/resolve
                  (fn [env input]
                    (let [ids (mapv id-attr-k input)
                          query (log/spy :debug
                                         :to-many-query
                                         (sql/format
                                          {:select select
                                           :from table
                                           :where [:in relationship-column ids]
                                           :group-by [relationship-column]}))
                          rows (sql.query/eql-query! env
                                                     query
                                                     schema
                                                     input)
                          results-by-id (reduce (fn [acc {:keys [k v]}]
                                                  (assoc acc k
                                                         {attr-k (mapv (fn [v]
                                                                         {target-k v})
                                                                       v)}))
                                                {}
                                                rows)
                          results (mapv #(get results-by-id %) ids)]

                      (auth/redact env results)))
                  ::pco/input [id-attr-k]}
           transform (assoc ::pco/transform transform)))]
    entity-by-attribute-resolver))

(defn to-one-resolvers [{::attr/keys [schema]
                         ::pco/keys [transform] :as attr
                         ::rad.sql/keys [ref] ;; question.index-card/question
                         attr-k ::attr/qualified-key ;; :question/index-card
                         target-k ::attr/target} ;; question.index-card/id
                        id-attr-k ;; question/id
                        id-attr->attributes
                        k->attr]
  (if-not ref
    ;; when there is no ref that means that the table has a
    ;; column with the ref.
    ;; so we only need to alias that ref attribute to reuse
    ;; the id-resolver of the ref entity.
    (pbir/alias-resolver (to-one-keyword attr-k) target-k)
    (let [{::attr/keys [schema]
           ::pco/keys [transform]} attr
          ref-attr (k->attr ref)
          target-attr (k->attr target-k)
          column->outputs (get-column->outputs target-k id-attr->attributes k->attr true)
          column-mapping (get-column-mapping column->outputs)
          outputs (vec (flatten (vals column->outputs)))
          columns (keys column->outputs)
          table (get-table target-attr)
          ref-column (get-column ref-attr)
          op-name (symbol
                   (str (namespace target-k))
                   (str "by-" (namespace id-attr-k) "-" (name id-attr-k) "-resolver"))
          one-to-one-resolver
          (pco/resolver
           op-name
           (cond->
               {::pco/output (conj outputs {attr-k outputs})
                ::pco/batch?  true
                ::pco/resolve
                (fn [env input]

                  (let [ids (mapv id-attr-k input)
                        query (sql/format {:select columns
                                           :from table
                                           :where [:in ref-column ids]})
                        rows (sql.query/eql-query! env
                                                   query
                                                   schema
                                                   input)
                        results-by-id (reduce
                                       (fn [acc row]
                                         (let [result
                                               (reduce
                                                (fn [acc [output-path column]]
                                                  (assoc-in acc output-path (get row column)))
                                                {}
                                                column-mapping)]
                                           (assoc acc
                                                  (get-in result [ref id-attr-k])
                                                  result)))
                                       {}
                                       rows)
                        results (mapv (fn [id]
                                        (when-let [outputs (get results-by-id id)]
                                          (assoc outputs attr-k outputs)))
                                      ids)]
                    (auth/redact env
                                 results)))
                ::pco/input [id-attr-k]}
             transform (assoc ::pco/transform transform)))]
      one-to-one-resolver)))

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
        ;; id-resolvers are basically a get by id, but they also handle batches, so that
        ;; one can ask for more than one id in a single query.
        ;; - we build one id resolver for each id attribute defined
        ;; - each resolvers grabs all the attributes defined for that particular identity
        ;; in the select clause.
        _ (log/debug "Generating resolvers for id attributes")
        id-resolvers
        (reduce-kv
         (fn [resolvers id-attr attributes]
           (conj resolvers (id-resolver {::attr/id-attr id-attr
                                         ::attr/id-attr->attributes id-attr->attributes
                                         ::attr/attributes   attributes
                                         ::attr/k->attr      k->attr})))
         []
         id-attr->attributes)

        ;; there are two types of target resolvers: one and many
        ;;
        target-resolvers
        (->> attributes
             (filter #(and
                       (= schema (::attr/schema %))
                       (#{:one :many} (::attr/cardinality %))))
             (mapcat
              (fn [{::attr/keys [cardinality] :as attribute}]
                (for [id-attr-k (::attr/identities attribute)]
                  (case cardinality
                    :one (to-one-resolvers attribute
                                           id-attr-k
                                           id-attr->attributes
                                           k->attr)
                    :many (to-many-resolvers attribute
                                             id-attr-k
                                             id-attr->attributes
                                             k->attr)))))
             )]
    (vec (concat id-resolvers target-resolvers))))
