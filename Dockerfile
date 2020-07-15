FROM akariv/dgp-app

COPY configuration.json dags/
COPY logo.png ui/dist/ui/assets/logo.png

COPY taxonomies taxonomies
COPY datacity_server datacity_server
COPY setup.py .

RUN pip install . 
RUN pip install "dgp>=0.0.61"
# RUN mv dags/operators/dgp_kind dags/operators/datacity
# # COPY migrate.py .
# # COPY trigger.py .
# COPY backup backup