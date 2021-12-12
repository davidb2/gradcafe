# GradCafe Scraper

Spin up the database using:
```
docker-compose up --detach db
```

Start the scraper using:
```
docker-compose up scrape
```

The `scrape` service uses the `SEEDS` environment variable in the `.env` file and writes to the database.


## Note

Please keep the `SEEDS` short as to not bombard the `gradcafe` server.