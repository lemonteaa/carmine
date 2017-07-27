(ns taoensso.carmine.cluster
  "EXPERIMENTAL support for Redis Cluster atop Carmine."
  {:author "Ben Poweski"}
  (:require [clojure.string :as str]
            [taoensso.encore  :as encore]
            [taoensso.carmine :as car]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]))


;; TODO Migrate to new design

;;; Description of new design:
;; The new design should be significantly more flexible + performant for use
;; with Cluster:
;; * `protocol/*context*` now contains a request queue (atom []).
;; * Redis commands previously wrote directly to io buffer, they now push
;;   'requests' to this queue instead.
;; * A 'request' looks like ["GET" "my-key" "my-val"] and has optional
;;   metadata which includes `:expected-keyslot` - hashed cluster key (crc16).
;;
;; * Request pushing + metadata is all handled by `commands/enqueue-request`.
;;
;; * Before actually fetching server replies, all queued requests are written to
;;   io buffer with `protocol/execute-requests`.
;;
;; * This fn works with dynamic arguments (default), or with an explicit
;;   Connection and requests. It is fast + flexible (gives us a lot of room to
;;   make Cluster-specific adjustments).

;;; Sketch of suggested implementation:
;; * `:cluster` should be provided as part of connection :spec options.
;; * The `protocol/execute-requests` fn could be modded so that when the
;;   :cluster key is present, behaviour is delegated to
;;   `cluster/execute-requests`.
;; * This fn groups requests by the (cached) keyslot->server info:
;;   (let [plan (group-by <grouping-fn> requests)]
;;    <...>).
;; * Futures can then call `protocol/execute-requests` with the explicit
;;   expected connections + planned requests & deliver replies to promises.
;; * All Cluster replies could be inspected for Cluster errors like MOVE, etc.
;;   This is easier than before since exceptions now have
;;   `(ex-data <ex>)` with a :prefix key that'll be :moved, :ack, :wrongtype, etc.
;; * After all promises have returned, we could regroup for any moved keys and
;;   loop, continuing to accumulate appropriate replies.
;; * We eventually return all the replies in the same order they were provided.
;;   Parsers & other goodies will just work as expected since all that info is
;;   attached to the requests themselves.

;;; Cache data structure and functions

(defprotocol async-cache
  (get-item [this i] "Retrieve item by index")
  (update-one! [this i value] "Update item at index, return true if modified")
  (update-all! [this info] "Replace the entire cache"))

(defrecord VectorAsyncCache [x]
  async-cache
  (get-item [this i] (get x i))
  (update-one! [this i value] (swap! x (fn [y] (assoc y i value))))
  (update-all! [this info] (do))) ;TODO

;; {<name> {<keyslot> <conn-spec>}}
(def ^:private cached-keyslot-conn-specs (atom {}))

;;; util

(defn singleton-future
  [lock f]
  (if (compare-and-set! lock false true)
    (future
      f
      (reset! lock false))
    nil))

; see https://stackoverflow.com/questions/9638271/update-the-values-of-multiple-keys
(defn update-vals [map vals f]
  "Update multiple values in map"
  (reduce #(update-in % [%2] f) map vals))

(defn ranges [rs]
  "Enumerate all items in a list of ranges"
  (mapcat (partial apply #(range %1 (inc %2))) rs))

(defn as-slots-array [nodes]
  (reduce (fn [acc node]
            (update-vals acc (ranges (:slots node))
                         #(conj % (:spec node))))
          (vec (repeat 16384 ()))
          nodes))

(defn cluster-err [e]
  "Parse exceptions for MOVED and ASK by adding redirect info to ex-data"
  { :parse-exceptions? true }
  (case (:prefix (ex-data e))
        (:moved :ask) (let [[_ slot addr] (str/split (.getMessage e) #" ")
                            [host port]   (str/split addr #":")
                            port          (car/as-long port)]
                        (ex-info (.getMessage e)
                                 (merge (ex-data e)
                                        {:loc {:host host :port port} :slot slot})))
    e))


(defn parse-slots [s]
  (when-not (re-find #"^\[" s)
	(let [[start stop] (str/split s #"-")
		  stop         (or stop start)]
	  [(car/as-long start) (car/as-long stop)])))

(defn parse-spec [addr]
  (let [[host port] (str/split addr #":")
		bound-spec  (get-in protocol/*context* [:conn :spec])]
	(if (str/blank? host)
	  bound-spec
	  (merge bound-spec {:host host :port (car/as-long port)}))))

(defn parse-cluster-node [s]
  (let [fields (str/split s #" ")
		[name addr flags master-id ping-sent ping-recv config-epoch link-status & slots] fields
		spec (parse-spec addr)]
	{:spec      (parse-spec addr)
	 :slots     (map parse-slots slots)
	 :flags     (set (map keyword (str/split flags #",")))
	 :replicate (if (= master-id "-") false master-id)
	 :ping-sent (car/as-long ping-sent)
	 :ping-recv (car/as-long ping-recv)}))


;;----

(defn retryable? [obj]
  (= (:prefix (ex-data obj)) :moved))

(def max-retries 14)

(defn parse-redirect [msg]
  (let [[_ slot addr] (str/split msg #" ")]
    (when-let [[host port] (str/split addr #":")]
      [(encore/as-int slot) host (encore/as-int port)])))

(defn find-spec [m slot conn]
  (get-in m [(get-in conn [:spec :cluster]) slot] (:spec conn)))

(defn send-request [conn-opts requests get-replies? replies-as-pipeline?]
  (let [[pool conn] (conns/pooled-conn conn-opts)]
    (try
      (let [response (protocol/execute-requests conn requests get-replies? replies-as-pipeline?)]
        (conns/release-conn pool conn)
        response)
      (catch Exception e
        (conns/release-conn pool conn e)
        e))))

(defn keyslot-specs [cluster coll]
  (into {} (for [[request e] coll
                 :let [[slot host port] (parse-redirect (.getMessage ^Exception e))]]
             [slot {:host host :port port :cluster cluster}])))

(defn- unpack [responses err]
  (let [ungrouped (group-by (comp retryable? last)
                            (for [[requests p] responses
                                  [request response] (map vector requests (deref p 5000 err))]
                              [request response]))]
    [(get ungrouped true) (get ungrouped false)]))

(defn execute-requests [conn requests get-replies? replies-as-pipeline?]
  (let [nreqs       (count requests)
        cluster     (get-in conn [:spec :cluster])
        requests    (map-indexed #(vary-meta %2 assoc :pos %1 :expected-keyslot (commands/keyslot (second %2))) requests) ;; comes through as nil?, temporary fix
        group-fn    (fn [request]
                      (find-spec (deref cached-keyslot-conn-specs) (:expected-keyslot (meta request)) conn))
        placeholder (java.util.concurrent.TimeoutException.)]

    (loop [plan   (group-by group-fn requests)
           result (vec (repeat nreqs placeholder))
           n      0]
      (let [responses (for [[spec reqs] plan]
                        [reqs (future (if (> (count reqs) 1)
                                        (send-request {:spec spec} reqs get-replies? replies-as-pipeline?)
                                        [(send-request {:spec spec} reqs get-replies? replies-as-pipeline?)]))])
            [remaining done] (unpack responses placeholder)
            result (reduce (fn [coll [request response]]
                             (assoc coll (:pos (meta request)) response))
                           result
                           done)]

        (swap! cached-keyslot-conn-specs update-in [cluster] merge (keyslot-specs cluster remaining))

        (cond (and (seq remaining) (< n max-retries)) (recur (group-by group-fn (map first remaining)) result (inc n))
              (> nreqs 1) result
              :else (first result))))))

(comment ; Example

  ;; Step 1:
  (car/wcar {:spec {:host "127.0.0.1" :port 7001 :cluster "foo"}}
    (car/get "key-a")
    (car/get "key-b")
    (car/get "key-c"))

  (car/wcar {:spec {:host "127.0.0.1" :port 7002 :cluster "foo"}}
    (car/get "key-f")
    (car/get "key-a")
    (car/get "key-b")
    (car/get "key-l"))

  (time (dotimes [n 1000]
          (car/wcar {:spec {:host "127.0.0.1" :port 7001 :cluster "foo"}}
            (doseq [n (range 100)]
              (car/get (str "key-" n))))))

  ;; Step 2:
  ;; protocol/execute-requests will receive requests as:
  ;; [["GET" "key-a"] ["GET" "key-b"] ["GET" "key-c"]]
  ;; Each will have :expected-keyslot metadata.
  ;; None have :parser metadata in this case, but it wouldn't make a difference
  ;; to our handling here.

  ;; Step 3:
  ;; Does our cache know which servers serve each of the above slots?[1]
  ;; Group requests per server + `execute-requests` in parallel with
  ;; appropriate Connections specified (will override the dynamic args).

  ;; Step 4:
  ;; Wait for all promises to be fulfilled (a timeout may be sensible).

  ;; Step 5:
  ;; Identify (with `(:prefix (ex-data <ex>))`) which replies are Cluster/timeout
  ;; errors that imply we should try again.

  ;; Step 6:
  ;; Continue looping like this until we've got all expected replies or we're
  ;; giving up for certain replies.

  ;; Step 7:
  ;; Return all replies in order as a single vector (i.e. the consumer won't be
  ;; aware which nodes served which replies).

  ;; [1]
  ;; Since slots are distributed to servers in _ranges_, we can do this quite
  ;; efficiently.
  ;; Let's say we request a key at slot 42 and determine that it's at
  ;; 127.0.0.1:6379 so we cache {42 <server 127.0.0.1:6379>}.
  ;;
  ;; Now we want a key at slot 78 but we have no idea where it is. We can scan
  ;; our cache and find the nearest known slot to 78 and use that for our first
  ;; attempt. So with n nodes, we'll have at most n-1 expected-slot misses.
  )

;;; Older iteration, seems to still have some useful pieces that can be referenced.
(comment

	(def ^:dynamic *conn* nil)

	(def ^:private slot-cache (atom {}))

	(defn cached-conn [cluster key]
	  (get-in @slot-cache [cluster (keyslot key)]))

	(defn replace-conn! [cluster slot spec]
	  (get-in (swap! slot-cache assoc-in [cluster slot] {:spec spec :cluster cluster})
			  [cluster slot]))

	(defn parse-redirect [error]
	  (let [[error slot address] (str/split error #" ")]
		[(car/as-long slot) (parse-spec address)]))

	(defn moved? [^Exception exception]
	  (if-let [error (.getMessage exception)]
		(.startsWith error "MOVED")
		false))

	(defn try-request [conn args]
	  (try
		(car/wcar conn (protocol/send-request* args))
		(catch Exception e e)))

	(defn send-request*
	  "Sends a request to a Redis Cluster, follow redirects if the key has moved."
	  [args]
	  (let [cluster (:cluster *conn*)]
		(loop [redirects 0
			   conn      *conn*]
		  (let [response (try-request (or (cached-conn cluster (second args)) *conn*) args)]
			(cond (not (instance? Exception response)) response
				  (> redirects 15) (Exception. "too many cluster redirects")
				  (moved? response) (recur (inc redirects) (apply replace-conn! cluster (parse-redirect (.getMessage ^Exception response))))
				  :else
				  response)))))

	(defmacro ccar
	  "Evaluates body in the context of multiple thread-bound pooled connections to Redis
	  cluster. Sends Redis commands to server as pipeline and returns the server's
	  response. Releases connection back to pool when done.

	  `conn` arg is a map with connection pool, spec and cluster options:
		{:pool {} :spec {:host \"127.0.0.1\" :port 7000} :cluster \"my-cluster\"}"
	  {:arglists '([conn :as-pipeline & body] [conn & body])}
	  [conn & sigs]
	  `(binding [protocol/send-request send-request*
				 *conn* ~conn]
		 (vector ~@sigs)))

	(comment (ccar {:spec {:host "127.0.0.1" :port 7001} :cluster "my-cluster"} (car/get "foo")))

	(defn cluster-nodes*
	  "Queries the current list of cluster nodes."
	  []
	  (->> (protocol/send-request [:cluster :nodes])
		   (parse
			(fn [reply]
			  (mapv parse-cluster-node (str/split-lines reply))))))


	(comment (clojure.pprint/pprint (car/wcar {:spec {:host "127.0.0.1" :port 7000 :stuff "here"}} (cluster-nodes*))))

)
