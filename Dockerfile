FROM akariv/dgp-ui

RUN mv dist/kuvira/he he && rm -rf dist/kuvira/ && mv he dist/kuvira
COPY logo-transparent.png dist/kuvira/assets/img/logo-transparent.png

