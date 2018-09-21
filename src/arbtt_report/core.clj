(ns arbtt-report.core
  (:require [cheshire.core :refer :all]
            [java-time :as time]
            [clojure.java.jdbc :as jdbc]))

(def db-spec
   {:classname   "org.h2.Driver"
    :subprotocol "h2:mem"
    :subname     "demo;DB_CLOSE_DELAY=-1"
    :user        "sa"
    :password    ""})

#_(jdbc/db-do-commands db-spec
    (jdbc/drop-table-ddl :arbtt_sample))

(jdbc/db-do-commands db-spec
  (jdbc/create-table-ddl
    :arbtt_sample
    [[:id "bigint primary key auto_increment"]
     [:date_time "timestamp"]
     [:rate "bigint"]
     [:inactive "bigint"]
     [:win_title "varchar(1000)"]
     [:win_program "varchar(1000)"]]))

(defn insert-samples
  [samples]
  (doseq
    [vals (partition-all 100 samples)]
    (jdbc/insert-multi!
      db-spec
      :arbtt_sample
      [:date_time :rate :inactive :win_title :win_program]
      (map (fn
            [{:keys [date_time rate inactive win_title win_program]}]
            [date_time rate inactive win_title win_program])
           vals))))

(defn coerce-sample
  [sample]
  (let [active-window (first (filter :active (:windows sample)))]
    (->
      sample
      (assoc :date_time (java.time.Instant/parse (:date sample)))
      (assoc :win_title (:title active-window))
      (assoc :win_program (:program active-window)))))

(defn parse-samples
  [path]
  (let [samples (parse-stream (clojure.java.io/reader path) true)]
    (map coerce-sample samples)))

(defn active?
  [sample]
  (< (:inactive sample) (:rate sample)))

(insert-samples
   (filter active? (parse-samples "/home/remo/arbtt.json")))

(jdbc/delete! db-spec :arbtt_sample ["1=1"])

(take 10
  (jdbc/query db-spec
    "select * from arbtt_sample s
     where formatdatetime(date_time, 'yyyy-MM-dd')='2017-10-09'
     and exists (select 1 from arbtt_sample x
                            where x.id=s.id-1 and
                            x.date_time>timestampadd('SECOND',-65,s.date_time))
     order by date_time desc"))

(->>
  (jdbc/query db-spec
    "select formatdatetime(s.date_time, 'yyyy-MM-dd') date,
            min(s.date_time) start,
            max(s.date_time) end,
            --count(1) cnt,
            datediff('MINUTE', min(s.date_time), max(s.date_time)) diff
     from arbtt_sample s
     where exists (select 1 from arbtt_sample x
                            where x.id=s.id-10 and
                            x.date_time>timestampadd('HOUR',-2,s.date_time))
     group by formatdatetime(s.date_time, 'yyyy-MM-dd')
     having count(1)>60
     order by 1 desc")
  (map vals)
  (map #(clojure.string/join ";" %)))
