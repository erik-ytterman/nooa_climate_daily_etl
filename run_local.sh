rm -Rfv ./outdata
java -jar ./target/NooaClimateDailyETL-1.0-SNAPSHOT.jar \
    ./samples/ghcnd-daily-error.jsonl \
    ./outdata/ \
    ./schemas/ghcnd-daily.avsc \
    ./schemas/ghcnd-daily.jsons \
    ./errors
