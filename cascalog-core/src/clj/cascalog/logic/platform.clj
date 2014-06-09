(ns cascalog.logic.platform
  "The execution platform class, plus a basic Cascading implementation."
  (:require [jackknife.core :as u]
            [cascalog.cascading.operations :as ops]
            [cascalog.cascading.types :as types]))

(defprotocol IPlatform
  (generator? [p x]
    "Returns true if the supplied x is a generator, false
    otherwise.")
  (generator [p gen fields options]
    "Returns some source representation."))

(defn- init-pipe-name [options]
  (or (:name (:trap options))
      (u/uuid)))

(defn- init-trap-map [options]
  (if-let [trap (:trap options)]
    {(:name trap) (types/to-sink (:tap trap))}
    {}))

(defrecord CascadingPlatform []
  IPlatform
  ;; Given a variable x tell whether or not x is a generator
  (generator? [_ x]
    (satisfies? types/IGenerator x))

  (generator [_ gen fields options]
    (-> (types/generator gen)
        (update-in [:trap-map] #(merge % (init-trap-map options)))
        (ops/rename-pipe (init-pipe-name options))
        (ops/rename* fields)
        (ops/filter-nullable-vars fields))))

(def ^:dynamic *context* (CascadingPlatform.))

(defn gen? [g]
  (generator? *context* g))

;;(defrecord ClojurePlatform []
  "Clojure in memory platform for cascalog"
;;  IPlatform
  ;; answers the question is this a generator?
;; (generator? [_ x]
;;    (satisfies? types/IGenerator))
  ;; TODO PWM figure out how to represent the source for an in memory
  ;; platform
;;  (generator [p gen fields options]))


(comment
  (require '[cascalog.cascading.flow :as f])
  "TODO: Convert to test."
  (let [gen (-> (types/generator [1 2 3 4])
                (ops/rename* "?x"))
        pred (to-predicate * ["?a" "?a"] ["?b"])]
    (fact
     (f/to-memory
      ((:op pred) gen ["?x" "?x"] "?z"))
     => [[1 1] [2 4] [3 9] [4 16]])))
