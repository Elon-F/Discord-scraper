# Realtime Discord Scraper
For all your discord-scraping needs.

### Details

Uses Discord's API to read all incoming messages, then saves all new and existing messages in the target channels to your database of choice.  

Supports both bot accounts, and regular user accounts.

To obtain your regular user account's token, run the following in the web console with the discord app open (or look at the headers of XHR requests):

```javascript
(webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m => m?.exports?.default?.getToken).exports.default.getToken()
```

### Installation

Clone the repository, then build the image with either `docker build` or `docker-compose`.

Currently, supports only MongoDB which must be running and set the environment variables appropriately. 
I recommend using the official mongoDB docker image for this purpose. 

#### Docker-compose: 
You can set the environment variables via an env file, or directly in the docker-compose file:
```yaml
    environment:
      - MONGO_HOST=mongo.host
      - MONGO_PORT=27017
      - DISCORD_TOKEN=JSON.Web.Token
      - DISCORD_BOT=false
      - TARGET_CHANNELS=123, 345, 456
```