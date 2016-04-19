yarn jar ./target/NooaClimateDailyETL-1.0-SNAPSHOT.jar \
    /user/cloudera/climate-2015-12-15/jsondata/daily/ghcnd-daily-error.jsonl \
    /user/cloudera/climate-2015-12-15/outdata/daily \
    /user/cloudera/climate-2015-12-15/schemas/ghcnd-daily.avsc \
    /user/cloudera/climate-2015-12-15/schemas/ghcnd-daily.jsons \
    /user/cloudera/climate-2015-12-15/errors