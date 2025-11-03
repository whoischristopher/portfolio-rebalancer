Disclaimer:
This project has been constructed mostly through vibe-coding with no manual code review.  No warranty is implied - use as it is.   

This is a rudimentary attempt at a simple web application that will help rebalance a stock portfolio against a desired asset allocation target.

Key features:
- Google OAuth authentication
- Private and public security products that are tied to asset classes.  Public security price updates via yFianance.
- Securities can be restricted to specific accounts, a preference model, or unrestricted (any account)
- Multi account types: Registered and non-registered.   Future features will take advantage of the tax-exempt nature of registered accounts.
- Asset Allocation targets
- Rebalance against the targets

To be implemented:
- Taking tax implication into consideration for rebalancing
- fix security price update
- fix rebalancing logic

Installation:

- Clone/pull the code
- Implement Google OAuth and obtain your Google_Client ID and Secret
- Create docker-compose.yml (using sample beblow and modify as necessary), and off you go.

sample docker-compose.yml:
````
services:
  web:
    build: .
    container_name: portfolio_rebalancer
    ports:
      - "8080:5000"
    environment:
      - FLASK_ENV=production
      - SECRET_KEY=${SECRET_KEY}
      - GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID}
      - GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET}
      - DATABASE_URL=${DATABASE_URL}
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
