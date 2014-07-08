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
  ))

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

#_(defmethod p/to-predicate Function
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

#_(defmethod op-im Function
  "IM operation application of a function. IM equiv to CascadingFunctionWrapper." 
  [op gen input output]
  )

#_(defmethod op-im CascalogFunction
  "IM operation application of a CascalogFunction. IM equiv to CascalogFunctionWrapper." 
  [op gen input output]
  )

#_(defmethod op-cascading ::d/map
 "IM operation application of a maping of an op. IM equiv to cascading.operations/map*"
  [op gen input output]
  )

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
