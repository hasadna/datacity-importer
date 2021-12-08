FROM akariv/dgp-app:latest

COPY requirements.dev.txt .
RUN sudo pip install -U -r requirements.dev.txt

COPY configuration.json dags/
COPY logo.png ui/dist/ui/assets/logo.png

COPY taxonomies taxonomies
COPY datacity_server datacity_server
COPY operators dags/operators/
COPY setup.py .

RUN pip install . 
