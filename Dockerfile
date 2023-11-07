FROM python:3

WORKDIR /home/scraper/
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py ./
COPY entrypoint.sh ./

ENV MONGO_HOST="host.docker.internal" \
    MONGO_PORT=27017 \
    DISCORD_BOT=false \
    MESSAGE_FETCH_LIMIT=500

ENTRYPOINT ["./entrypoint.sh"]
CMD python3 discord_scraper.py