PAYLOADS=(0 1 5 10)
VALIDATED=(0 4 8 12)
OPS=(100)
mkdir "kabis"
mkdir "kafka"
for payloadKB in "${PAYLOADS[@]}"; do
  ((payloadB=1024*payloadKB))
  for ops in "${OPS[@]}"; do
    for validated in "${VALIDATED[@]}"; do
      file="kabis/$payloadKB-$validated.env"
      echo "SENDER_OPS=$ops">"$file"
      echo "PAYLOAD=$payloadB">>"$file"
      echo "VALIDATED=$validated">>"$file"
      source ../../../.env
      echo "DOCKERHUB_USERNAME=$DOCKERHUB_USERNAME" >>"$file"
    done
    file="kafka/$payloadKB.env"
    echo "SENDER_OPS=$ops">"$file"
    echo "PAYLOAD=$payloadB">>"$file"
    source ../../../.env
    echo "DOCKERHUB_USERNAME=$DOCKERHUB_USERNAME" >>"$file"
  done
done

