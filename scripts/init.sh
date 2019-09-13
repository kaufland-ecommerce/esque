# All credit to: https://github.com/wurstmeister/kafka-docker/blob/master/start-kafka.sh

function updateConfig() {
    key=$1
    value=$2
    file=$3

    # If config exists in file, replace it. Otherwise, append to file.
    if grep -E -q "^#?$key=" "$file"; then
        sed -r -i "s@^#?$key=.*@$key=$value@g" "$file"
        echo "[Replaced] '$key' in '$file' [$key=$value]"
    else
        printf "\n%s=%s\n" "$key" "$value" >> "$file"
        echo "[Appended] '$key' in '$file' [$key=$value]"
    fi
}

# Read in env as a new-line separated array. This handles the case of env variables have spaces and/or carriage returns. See #313
IFS=$'\n'
for VAR in $(env)
do
    env_var=$(echo "$VAR" | cut -d= -f1)
    if [[ "$EXCLUSIONS" = *"|$env_var|"* ]]; then
        echo "Excluding $env_var from broker config"
        continue
    fi

    if [[ $env_var =~ ^KAFKA_ || $env_var =~ ^ZOOKEEPER_ || $env_var =~ ^SCHEMAREGISTRY_ ]]; then
        kafka_name=$(echo "$env_var" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .)
        updateConfig "$kafka_name" "${!env_var}" "$CONFIG_PATH"
    fi
done