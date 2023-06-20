NUM_ART_EXHIBITIONS=(1 5 10 15)
NUM_TRUE_ALARMS=(50)
NUM_FALSE_ALARMS=(10)
NUM_UNCAUGHT_BREACHES=(10)

for art_exhibitions in "${NUM_ART_EXHIBITIONS[@]}"; do
  for true_alarms in "${NUM_TRUE_ALARMS[@]}"; do
    for false_alarms in "${NUM_FALSE_ALARMS[@]}"; do
      for uncaught_breaches in "${NUM_UNCAUGHT_BREACHES[@]}"; do
        file="$art_exhibitions-$true_alarms-$false_alarms-$uncaught_breaches.env"
        echo "NUM_EXHIBITIONS=$art_exhibitions">"$file"
        echo "NUM_TRUE_ALARMS=$true_alarms">>"$file"
        echo "NUM_FALSE_ALARMS=$false_alarms">>"$file"
        echo "NUM_UNCAUGHT_BREACHES=$uncaught_breaches">>"$file"
        source ../../../../.env
        echo "DOCKERHUB_USERNAME=$DOCKERHUB_USERNAME" >>"$file"
        done
    done
  done
done

