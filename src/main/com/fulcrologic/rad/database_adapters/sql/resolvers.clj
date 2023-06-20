(ns com.fulcrologic.rad.database-adapters.sql.resolvers
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
    [next.jdbc.sql :as jdbc.sql]
    [honey.sql :as sql]
    [clojure.spec.alpha :as s]
    [next.jdbc :as jdbc]

    ;; IMPORTANT: This turns on instant coercion:
    [next.jdbc.date-time]

    [com.fulcrologic.fulcro.algorithms.tempid :as tempid]
    [com.fulcrologic.rad.ids :as ids]
    [clojure.string :as str]
    [clojure.set :as set]
    [edn-query-language.core :as eql]
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]
    [com.fulcrologic.rad.attributes :as rad.attr]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reads

(defn entity-query
  "The entity query used by the pathom resolvers."
  [{::attr/keys [id-attribute key->attribute] :as env} input]
  (enc/if-let [query* (get env ::rad.sql/default-query)]
    (let [result (sql.query/eql-query! env id-attribute query* input)]
      result)
    (do
      (log/info "Unable to complete query.")
      nil)))

(>defn attributes->eql
       "Returns an EQL query for all of the attributes that are available for the given database-id"
       [attrs]
       [::attributes => vector?]
       (reduce
        (fn [outs {::keys [qualified-key type target cardinality]}]
          (if (= :many cardinality)
            outs
            (conj outs qualified-key)))
        []
        attrs))


(defn id-resolver [{::attr/keys [id-attribute attributes k->attr]}]
  (enc/if-let [id-key  (::attr/qualified-key id-attribute)
               outputs (attributes->eql attributes)
               schema  (::attr/schema id-attribute)]
    (let [transform (:com.wsscode.pathom.connect/transform id-attribute)]
      (cond-> {:com.wsscode.pathom.connect/sym     (symbol
                                                    (str (namespace id-key))
                                                    (str (name id-key) "-resolver"))
               :com.wsscode.pathom.connect/output  outputs
               :com.wsscode.pathom.connect/batch?  true
               :com.wsscode.pathom.connect/resolve (fn [env input]
                                                     (auth/redact env
                                                                  (log/spy :trace (entity-query
                                                                                   (assoc env
                                                                                          ::attr/id-attribute id-attribute
                                                                                          ::attr/schema schema
                                                                                          ::rad.sql/default-query outputs)
                                                                                   (log/spy :trace input)))))
               :com.wsscode.pathom.connect/input   #{id-key}}
        transform transform))
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writes

(defn resolve-tempid-in-value [tempids v]
  (cond
    (tempid/tempid? v) (get tempids v v)
    (eql/ident? v) (let [[k id] v]
                     (if (tempid/tempid? id)
                       [k (get tempids id id)]
                       v))
    :else v))

(def keys-in-delta
  (fn keys-in-delta [delta]
    (let [id-keys  (into #{}
                     (map first)
                     (keys delta))
          all-keys (into id-keys
                     (mapcat keys)
                     (vals delta))]
      all-keys)))

(defn schemas-for-delta [{::attr/keys [key->attribute]} delta]
  (let [all-keys (keys-in-delta delta)
        schemas  (into #{}
                   (keep #(-> % key->attribute ::attr/schema))
                   all-keys)]
    schemas))

(defn- generate-tempids [ds key->attribute delta]
  (reduce
   (fn [result [table id]]
     (if (tempid/tempid? id)
       (let [{::attr/keys [type] :as id-attr} (key->attribute table)
             real-id (if (#{:int :long} type)
                       (:id (first (jdbc/execute! ds [(format "SELECT NEXTVAL('%s') AS id" (sql.schema/sequence-name id-attr))])))
                       (ids/new-uuid))]
         (assoc result id real-id))
       result))
   {}
   (keys delta)))

(defn- table-local-attr
  "Returns an attribute or nil if it isn't stored on the semantic table of the attribute."
  [k->a schema-to-save k]
  (let [{::attr/keys [type cardinality schema] :as attr} (k->a k)]
    (when (= schema schema-to-save)
      (cond
        (and (= cardinality :many) (= :ref type))
        nil

        (= cardinality :one)
        (if (::rad.sql/ref attr)
          nil
          attr)

        :else
        attr))))

(defn form->sql-value [{::attr/keys    [type cardinality]
                        ::rad.sql/keys [form->sql-value]} form-value]
  (cond
    (and (= :ref type) (not= :many cardinality) (eql/ident? form-value)) (second form-value)
    form->sql-value (form->sql-value form-value)
    (= type :enum) (str form-value)
    :else form-value))

(defn scalar-insert
  [{::attr/keys [key->attribute] :as env} schema-to-save tempids [table id :as ident] diff]
  (when (tempid/tempid? (log/spy :trace id))
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (if (= schema schema-to-save)
        (let [table-name    (sql.schema/table-name key->attribute id-attr)
              real-id       (get tempids id id)
              scalar-attrs  (keep
                              (fn [k]
                                (table-local-attr key->attribute schema-to-save k))
                              (keys diff))
              new-val       (fn [{::attr/keys [qualified-key schema] :as attr}]
                              (when (= schema schema-to-save)
                                (let [v (get-in diff [qualified-key :after])
                                      v (resolve-tempid-in-value tempids v)]
                                  (form->sql-value attr v))))
              values (reduce (fn [acc attr]
                               (let [v (new-val attr)]
                                 (if (nil? v)
                                   acc
                                   (assoc acc (sql.schema/column-name attr) v))))
                             {(sql.schema/column-name id-attr) real-id}
                             scalar-attrs)]
          (sql/format {:insert-into table-name
                       :values [values]
                       :returning [:*]}))
        (log/debug "Schemas do not match. Not updating" ident)        ))))

(defn delta->scalar-inserts [{::attr/keys    [key->attribute]
                              ::rad.sql/keys [connection-pools]
                              :as            env} schema delta]
  (let [ds      (get connection-pools schema)
        tempids (log/spy :trace (generate-tempids ds key->attribute delta))
        stmts   (keep (fn [[ident diff]] (scalar-insert env schema tempids ident diff)) delta)]
    {:tempids        tempids
     :insert-scalars stmts}))

(defn scalar-update
  [{::keys      [tempids]
    ::attr/keys [key->attribute] :as env} tempids schema-to-save [table id :as ident] diff]
  (when-not (tempid/tempid? id)
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (when (= schema-to-save schema)
        (let [table-name   (sql.schema/table-name key->attribute id-attr)]
          (if (:delete diff)
            (sql/format {:delete-from table-name
                         :where [:= (sql.schema/column-name id-attr) id]})
            (let [scalar-attrs (keep
                                 (fn [k] (table-local-attr key->attribute schema-to-save k))
                                 (keys diff))
                  old-val      (fn [{::attr/keys [qualified-key] :as attr}]
                                 (some->> (get-in diff [qualified-key :before])
                                          (form->sql-value attr)))
                  new-val      (fn [{::attr/keys [qualified-key schema] :as attr}]
                                 (when (= schema schema-to-save)
                                   (let [v (get-in diff [qualified-key :after])
                                         v (resolve-tempid-in-value tempids v)]
                                     (form->sql-value attr v))))
                  values (reduce
                          (fn [result attr]
                            (let [new      (log/spy :debug (new-val attr))
                                  col-name (keyword (sql.schema/column-name attr))
                                  old      (old-val attr)]
                              (cond
                                (= new :delete)
                                (reduced :delete)
                                (and old (nil? new))
                                (assoc result col-name nil)

                                (not (nil? new))
                                (assoc result col-name new)

                                :else
                                result)))
                          {}
                          scalar-attrs)]
              (def values values)
              (when (seq values)
                (sql/format {:update table-name
                             :set values
                             :where [:= (sql.schema/column-name id-attr) id]})))))))))

(defn delta->scalar-updates [env tempids schema delta]
  (vec (keep (fn [[ident diff]] (scalar-update env tempids schema ident diff)) delta)))

(defn process-attributes [key->attribute delta]
  (reduce (fn [acc [ident attributes]]
            (reduce (fn [acc [attr-k value]]
                      (let [{::rad.attr/keys [type cardinality]
                             ::rad.sql/keys [ref delete-referent?] :as attribute                             }
                            (key->attribute attr-k)
                            {:keys [before after]} value]
                        (if (and ref
                                 (not (= before after nil)))
                          (case cardinality
                            :one
                            (assoc-in acc [(:after value) ref] {:after ident})
                            :many
                            (let [after-set (into #{} after)
                                  before-set (into #{} before)]
                              (reduce (fn [acc ref-ident]
                                        (cond
                                          (and (after-set ref-ident)
                                               (not (before-set ref-ident)))
                                          (assoc-in acc [ref-ident ref] {:after ident})

                                          (and (before-set ref-ident)
                                               (not (after-set ref-ident)))
                                          (assoc-in acc [ref-ident ref] {:before ident
                                                                         :after (if delete-referent?
                                                                                  :delete
                                                                                  nil)})

                                          (and (after-set ref-ident)
                                               (before-set ref-ident))
                                          acc))
                                      acc
                                      (set/union after-set before-set))))
                          acc)))
                    acc
                    attributes))
          delta
          delta))

(defn save-form!
  "Does all the necessary operations to persist mutations from the
  form delta into the appropriate tables in the appropriate databases"
  [{::attr/keys    [key->attribute]
    ::rad.sql/keys [connection-pools adapters default-adapter]
    :as            env} {::rad.form/keys [delta] :as d}]
  (let [schemas (schemas-for-delta env delta)
        delta (process-attributes key->attribute delta)
        result  (atom {:tempids {}})]
    (def env env)
    (def new-d delta)
    (log/debug "Saving form acrosbs " schemas)
    (log/debug "SQL Save of delta " (with-out-str (pprint delta)))
    ;; TASK: Transaction should be opened on all databases at once, so that they all succeed or fail
    (doseq [schema (keys connection-pools)]
      (let [adapter        (get adapters schema default-adapter)
            ds             (get connection-pools schema)
            {:keys [tempids insert-scalars]} (log/spy :trace (delta->scalar-inserts env schema delta)) ; any non-fk column with a tempid
            _ (def tempids tempids)
            update-scalars (log/spy :trace (delta->scalar-updates env tempids schema delta)) ; any non-fk columns on entries with pre-existing id
            steps          (concat update-scalars insert-scalars)]
        (jdbc/with-transaction [tx ds {:isolation :serializable}]
          ;; allow relaxed FK constraints until end of txn
          (when adapter
            (vendor/relax-constraints! adapter tx))
          (doseq [stmt-with-params steps]
            (log/debug stmt-with-params)
            (jdbc/execute! tx stmt-with-params)))
        (swap! result update :tempids merge tempids)))
    @result))
