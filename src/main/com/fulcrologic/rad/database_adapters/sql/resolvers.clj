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
    [com.fulcrologic.rad.database-adapters.sql.vendor :as vendor]))

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

(defn tempid-in-value? [v]
  (cond
    (tempid/tempid? v) true
    (eql/ident? v) (let [[k id] v]
                     (if (tempid/tempid? id)
                       true
                       false))
    :else false))

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
                       (:id (first (jdbc/execute! ds [(format "SELECT max(id) from" (sql.schema/sequence-name id-attr))])))
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
      (when-not (and (= cardinality :many) (= :ref type))
        attr))))

(defn form->sql-value [{::attr/keys    [type cardinality]
                        ::rad.sql/keys [form->sql-value]} form-value]
  (cond
    (and (= :ref type) (not= :many cardinality) (eql/ident? form-value)) (second form-value)
    form->sql-value (form->sql-value form-value)
    (= type :enum) (str form-value)
    :else form-value))

(defn scalar-insert
  [ds {::attr/keys [key->attribute]
       ::rad.sql/keys [connection-pools] :as env} schema-to-save tempids [table id :as ident] diff]
  (when (tempid/tempid? (log/spy :trace id))
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (if (= schema schema-to-save)
        (let [table-name    (sql.schema/table-name key->attribute id-attr)
              scalar-attrs  (keep
                              (fn [k] (table-local-attr key->attribute schema-to-save k))
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
                                   (assoc acc (keyword (sql.schema/column-name attr)) v))))
                             {}
                             scalar-attrs)
              query (sql/format {:insert-into table-name
                                 :values [values]})
              result (jdbc/execute-one! ds query {:return-keys true})]
          (long (:insert_id result)))
        (log/debug "Schemas do not match. Not updating" ident)))))

(defn delta->scalar-inserts [{::attr/keys    [key->attribute]
                              ::rad.sql/keys [connection-pools]
                              :as            env} schema delta]
  (let [ds      (get connection-pools schema)]
    (reduce (fn [{:keys [tempids] :as acc} [[_ id :as ident] diff]]
              (if-let [insert-id (scalar-insert ds env schema tempids ident diff)]
                (assoc-in acc [:tempids id] insert-id)
                (assoc-in acc [:delta ident] diff)))
            {:delta {}
             :tempids {}}
            delta)))

(defn scalar-update
  [{::keys      [tempids]
    ::attr/keys [key->attribute] :as env} schema-to-save [table id :as ident] diff]
  (when-not (tempid/tempid? id)
    (let [{::attr/keys [type schema] :as id-attr} (key->attribute table)]
      (when (= schema-to-save schema)
        (let [table-name   (sql.schema/table-name key->attribute id-attr)
              scalar-attrs (keep
                             (fn [k] (table-local-attr key->attribute schema-to-save k))
                             (keys diff))
              old-val      (fn [{::attr/keys [qualified-key] :as attr}]
                             (some->> (get-in diff [qualified-key :before])
                                      (form->sql-value attr)))
              new-val      (fn [{::attr/keys [qualified-key schema required?] :as attr}]
                             (when (= schema schema-to-save)
                               (let [v (get-in diff [qualified-key :after])
                                     v (resolve-tempid-in-value tempids v)
                                     v (form->sql-value attr v)]
                                 (if (and (nil? v) required?)
                                   :delete
                                   v))))
              values (reduce
                      (fn [result attr]
                        (let [new      (log/spy :trace (new-val attr))
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
          (if (= values :delete)
            (sql/format {:delete-from table-name
                         :where [:= (keyword (sql.schema/column-name id-attr)) id]})
            (when (seq values)
              (sql/format {:update table-name
                           :set values
                           :where [:= (keyword (sql.schema/column-name id-attr)) id]}))))))))

(defn delta->scalar-updates [env schema delta]
  (let [stmts (vec (keep (fn [[ident diff]] (scalar-update env schema ident diff)) delta))]
    stmts))

(defn- ref-one-attr
  [k->a k]
  (let [{::attr/keys [type cardinality] :as attr} (k->a k)]
    (when (and (= :ref type) (not= cardinality :many))
      attr)))

(defn- ref-many-attr
  [k->a k]
  (let [{::attr/keys [type cardinality] :as attr} (k->a k)]
    (when (and (= :ref type) (= cardinality :many))
      attr)))

(defn to-one-ref-update [{::attr/keys [key->attribute] :as env} schema-to-save id-attr tempids row-id attr old-val [_ new-id :as new-val]]
  (when (= schema-to-save (::attr/schema attr))
    (let [table-name     (sql.schema/table-name key->attribute id-attr)
          {delete-on-remove? ::rad.sql/delete-referent?} attr
          id-column-name (sql.schema/column-name id-attr)
          target-id      (get tempids row-id row-id)
          new-id         (get tempids new-id new-id)]
      (cond
        new-id
        (sql/format {:update table-name
                     :set {(keyword (sql.schema/column-name attr)) new-id}
                     :where [:= (keyword (sql.schema/column-name id-column-name)) target-id]})

        old-val
        (if delete-on-remove?
          (let [foreign-table-id-key  (first old-val)
                old-id                (second old-val)
                foreign-table-id-attr (key->attribute foreign-table-id-key)
                foreign-table-name    (sql.schema/table-name key->attribute foreign-table-id-attr)
                foreign-id-column     (keyword (sql.schema/column-name key->attribute foreign-table-id-attr))]
            (sql/format {:delete-from foreign-table-name
                         :where [:= foreign-id-column old-id]}))
          (sql/format {:update table-name
                       :set {(keyword (sql.schema/column-name attr)) nil}
                       :where [:= (keyword (sql.schema/column-name id-column-name)) target-id]}))))))

(defn to-many-ref-update [schema tempids target-id to-many-attr old-val new-val]
  (when  (= schema (::attr/schema to-many-attr))
    (let [{delete-on-remove? ::rad.sql/delete-referent?} to-many-attr
          target-id       (get tempids target-id target-id)
          old-val (into #{} old-val)
          new-val (into #{} new-val)
          adds            (set/difference new-val old-val)
          removes         (set/difference old-val new-val)
          add-stmts (reduce (fn [acc id]
                              (assoc acc id {(::attr/qualified-key to-many-attr) {:after target-id}}))
                            {}
                            adds)]
      (reduce (fn [acc id]
                (assoc acc id {(::attr/qualified-key to-many-attr)
                               {:before target-id :after nil}}))
              add-stmts
              removes))))

(defn ref-updates [{::rad.sql/keys [one-to-many-relationships]
                    ::attr/keys [key->attribute] :as env} schema tempids [table id :as ident] diff]
  (let [id-attr           (key->attribute table)
        to-one-ref-attrs  (keep (fn [k] (ref-one-attr key->attribute k)) (keys diff))
        to-many-ref-attrs (keep (fn [k] (one-to-many-relationships k)) (keys diff))
        old-val           (fn [{{::attr/keys [qualified-key]} :target-attribute}] (get-in diff [qualified-key :before]))
        new-val           (fn [{{::attr/keys [qualified-key]} :target-attribute}] (get-in diff [qualified-key :after]))]

    (concat
     (keep (fn [a] (to-one-ref-update env schema id-attr tempids id a (old-val a) (new-val a))) to-one-ref-attrs)
     (mapcat (fn [a] (to-many-ref-update schema tempids id a (old-val a) (new-val a))) to-many-ref-attrs))))

(defn delta->ref-updates [env tempids schema delta]
  (reduce
   (fn [acc [ident diff]]
     (reduce-kv
      (fn [acc ident values]
        (update acc
                ident
                (fn [previous-values]
                  (reduce-kv
                   (fn [previous-values k {:keys [before after] :as v}]
                     (if (or after (not (some-> previous-values
                                                k
                                                :after)))
                       (assoc previous-values k v)
                       (assoc-in previous-values [k :before] before)))
                   previous-values
                   values))))
      acc
      (ref-updates env schema tempids ident diff)))
   delta
   delta))

(defn save-form!
  [{::attr/keys    [key->attribute]
    ::rad.sql/keys [connection-pools adapters default-adapter many-to-one-relationships]
    :as            env} {::rad.form/keys [delta] :as d}]
  (let [schemas (schemas-for-delta env delta)
        result  (atom {:tempids {}})]
    (log/debug "Saving form across " schemas)
    (log/debug "SQL Save of delta " (with-out-str (pprint delta)))
    ;; TASK: Transaction should be opened on all databases at once, so that they all succeed or fail
    (doseq [schema (keys connection-pools)]
      (let [adapter        (get adapters schema default-adapter)
            ds             (get connection-pools schema)]
        (jdbc/with-transaction [ds ds {:isolation :serializable}]
          ;; doing the scalar inserts
          ;; in the original library, the statements are pre-calculated.
          ;; I could not find to know in advance the IDs of the entities being inserted
          ;; to resolve the tempids in the other queries.
          (let [{:keys [tempids delta]} (log/spy :trace (delta->scalar-inserts env schema delta))
                delta (delta->ref-updates env tempids schema delta)
                update-scalars (log/spy :trace (delta->scalar-updates (assoc env ::tempids tempids) schema delta))]
            ;; allow relaxed FK constraints until end of txn
            (when adapter
              (vendor/relax-constraints! adapter ds))
            (doseq [stmt-with-params update-scalars]
              (jdbc/execute! ds stmt-with-params))
            (swap! result update :tempids merge tempids)))))
    @result))

(defn delete-entity! [env params]
  (log/error "DELETE NOT IMPLEMENTED" params))
