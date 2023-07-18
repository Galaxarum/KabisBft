PAYLOADS=(0 256 512 1024 2048)
VALIDATED=(0 2 5 7 10)
OPS=(50000)

rm -rf kabis kafka

mkdir "kabis"
mkdir "kafka"

for payloadB in "${PAYLOADS[@]}"; do
  for ops in "${OPS[@]}"; do
    for validated in "${VALIDATED[@]}"; do
      file="kabis/$payloadB-$validated.env"
      echo "SENDER_OPS=$ops">"$file"
      echo "PAYLOAD=$payloadB">>"$file"
      echo "VALIDATED=$validated">>"$file"
      source ../../../../.env
      echo "DOCKERHUB_USERNAME=$DOCKERHUB_USERNAME" >>"$file"
    done
    file="kafka/$payloadB.env"
    echo "SENDER_OPS=$ops">"$file"
    echo "PAYLOAD=$payloadB">>"$file"
    source ../../../../.env
    echo "DOCKERHUB_USERNAME=$DOCKERHUB_USERNAME" >>"$file"
  done
done

