(ns cascalog.in-memory.im-platform
  (:require [jackknife.seq :as s]
            [jackknife.core :as u]
            [cascalog.in-memory.im-operations :as im-ops]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.def :as d]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.algebra :refer (sum)]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.fn :as serfn]
            [cascalog.logic.vars :as v]
            [cascalog.logic.parse :as parse]
            [cascalog.cascading.operations :refer (logically)]
            )
  (:import  
    [cascalog.logic.parse TailStruct Projection Application
     FilterApplication Grouping Join ExistenceNode
     Unique Merge Rename]
    [cascalog.logic.predicate RawSubquery FilterOperation
     Operation Aggregator]
    [cascalog.cascading.operations IAggregateBy IAggregator
     Inner Outer Existence]
    [cascalog.logic.def ParallelAggregator ParallelBuffer Prepared]
    [jcascalog Predicate]))

;; Reference assem* and filter-assem* for creating properly done filtered assemblies

#_(extend-protocol p/ICouldFilter
    in-memory.operation.Filter
    (filter? [_] true))


;; ## Allowed Predicates

#_(defmethod p/to-predicate Filter
    [op input output]
    (u/safe-assert (#{0 1} (count output)) "Must emit 0 or 1 fields from filter")
    (if (empty? output)
      (FilterOperation. op input)
      (Operation. op input output)))

(defmethod p/to-predicate Function
  [op input output]
  (Operation. op input output))

#_(defmethod p/to-predicate ParallelAgg
    [op input output]
    (Aggregator. op input output))

#_(defmethod p/to-predicate CascalogBuffer
    [op input output]
    (Aggregator. op input output))

(defmulti op-im 
  "Multimethod for the in-memory application of an operation"
  (fn [op gen input output]
    (type op)))

(defmulti filter-im
  "Multimethod for in-memory version of a filter"
  (fn [op gen input]
    (type op)))

(defmulti agg-im
  "Multimethod for in-memory version of aggregators"
  (fn [op input output]
    (type op)))

#_(defmethod op-im Filter
    "IM application of a filter operation"
    [op gen input output]
    ;;TODO Filter implementation of for im-ops)
    )

(defmethod op-im Function
  "IM operation application of a function. IM equiv to CascadingFunctionWrapper." 
  [op gen input output]
  (im-ops/im-map gen op input output))

#_(defmethod op-im CascalogFunction
    "IM operation application of a CascalogFunction. IM equiv to CascalogFunctionWrapper." 
    [op gen input output]
    )

(defmethod op-cascading ::d/map
  "IM operation application of a maping of an op. IM equiv to cascading.operations/map*"
  [op gen input output]
  (im-ops/im-map gen op input output))

#_(defmethod op-cascading ::d/mapcat
    "IM operation application of the mapcat of an op. IM equivalent to cascading.operations/mapcat*"
    [op gen input output]
    )

#_(defmethod filter-cascading ::d/filter
    [op gen input]

    ((filter-assem [in] (ops/filter* op in))

     gen input))

#_(defmethod filter-cascading cascading.operation.Filter
    [op gen input]

    ((filter-assem [in] (ops/add-op #(Each. % in op)))

     gen input))

#_(defmethod agg-cascading ::d/buffer
    [op input output]
    (ops/buffer op input output))

#_(defmethod agg-cascading ::d/bufferiter
    [op input output]
    (ops/bufferiter op input output))

#_(defmethod agg-cascading ::d/aggregate
    [op input output]
    (ops/agg op input output))

#_(defmethod agg-cascading ::d/combiner
    [op input output]
    (ops/parallel-agg op input output))

#_(defmethod agg-cascading ParallelAggregator
    [op input output]
    (ops/parallel-agg (:combine-var op) input output
                      :init-var (:init-var op)
                      :present-var (:present-var op)))

#_(defmethod agg-cascading ParallelAgg
    [op input output]
    (ops/parallel-agg (serfn/fn [l r]
                        (-> op
                            (.combine (s/collectify l)
                                      (s/collectify r))))
                      input output
                      :init-var (serfn/fn [x]
                                  (.init op (s/collectify x)))))

#_(defmethod agg-cascading CascalogBuffer
    [op input output]
    (reify ops/IBuffer
      (add-buffer [_ pipe]
        (Every. pipe (casc/fields input)
                (CascalogBufferExecutor. (casc/fields output) op)))))

#_(defmethod agg-cascading CascalogAggregator
    [op input output]
    (reify ops/IAggregator
      (add-aggregator [_ pipe]
        (Every. pipe (casc/fields input)
                (CascalogAggregatorExecutor. (casc/fields output) op)))))

(defn substitute-if
  "Returns [newseq {map of newvals to oldvals}]"
  [pred subfn aseq]
  (reduce (fn [[newseq subs] val]
            (let [[newval sub] (if (pred val)
                                 (let [subbed (subfn val)] [subbed {subbed val}])
                                 [val {}])]
              [(conj newseq newval) (merge subs sub)]))
          [[] {}] aseq))

(defn constant-substitutions
  "Returns a 2-vector of the form

   [new variables, {map of newvars to values to substitute}]"
  [vars]
  (substitute-if (complement v/cascalog-var?)
                 (fn [_] (v/gen-nullable-var))
                 (collectify vars)))

(defn insert-subs [flow sub-m]
  (if (empty? sub-m)
    flow
    (apply insert* flow (apply concat sub-m))))

(defn with-constants
  "Allows constant substitution on inputs."
  [gen in-fields f]
  (let [[new-input sub-m] (constant-substitutions in-fields)
        ignored (keys sub-m)
        gen (-> (insert-subs gen sub-m)
                (f new-input))]
    (if (seq ignored)
      (discard* gen (fields ignored))
      gen)))

(defn- replace-ignored-vars
  "Replaces all ignored variables with a nullable cascalog
  variable. "
  [vars]
  (map (fn [v] (if (= "_" v) (v/gen-nullable-var) v))
       (collectify vars)))

(defn not-nil? [& xs]
  (every? (complement nil?) xs))

(defn filter-nullable-vars
  "If there are any nullable variables present in the output, filter
  nulls out now."
  [flow fields]
  (if-let [non-null-fields (seq (filter v/non-nullable-var? fields))]
    (filter* flow #'not-nil? non-null-fields)
    flow))

(defn no-overlap? [large small]
  (empty?
   (intersection (set (collectify large))
                 (set (collectify small)))))

(defn im-logically
  "Accepts a flow, input fields, output fields and a function that
  accepts the same things and allows for the following features:

  Any variables not prefixed with !, !! or ? are treated as constants
  in the flow. This allows for (map* flow + 10 [\"?a\"] [\"?b\"]) to
  work properly and clean up its fields without hassle.

  Any non-nullable output variables (prefixed with ?) are removed from
  the flow.

  Duplicate input fields are allowed. It is currently NOT allowed to
  output one of the input variables. In Cascalog, this triggers an
  implicit filter; this needs to be supplied at another layer."
  [gen in-fields out-fields f]
  {:pre [(no-overlap? out-fields in-fields)]}
  (let [new-output (replace-ignored-vars out-fields)
        ignored (difference (set new-output)
                            (set (collectify out-fields)))]
    (with-constants gen in-fields
      (fn [gen in]
        (with-duplicate-inputs gen in
          (fn [gen in delta]
            (let [gen (-> gen
                          (f (fields in)
                             (fields new-output)))
                  gen (if-let [to-discard (not-empty
                                           (fields (concat delta ignored)))]
                        (discard* gen to-discard)
                        gen)]
              (filter-nullable-vars gen new-output))))))))
