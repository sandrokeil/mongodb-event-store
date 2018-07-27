#!/usr/bin/env bash

GREEN=`tput setaf 2`
RESET=`tput sgr0`

sleeping() {
    date
    IDLE_TIME=$1
    until [ $IDLE_TIME -lt 1 ]; do
        let IDLE_TIME-=1
        printf "${IDLE_TIME} "
        sleep 1
    done
    echo ""
}

docker-compose -f docker-compose-tests.yml up -d --no-recreate

docker-compose -f docker-compose-tests.yml ps

echo "${GREEN}Initialize cluster in T-5s${RESET}"
sleeping 5
docker-compose -f docker-compose-tests.yml exec mongodb0 sh init.sh

sleep 1
docker-compose -f docker-compose-tests.yml exec mongodb0 sh status.sh

echo "${GREEN}You can now run the unit tests with:${RESET}"
echo "${GREEN}docker-compose -f docker-compose-tests.yml run --rm php php vendor/bin/phpunit -c phpunit.xml.dist${RESET}"
