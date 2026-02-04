Disclaimer:
This project has been constructed mostly through vibe-coding with no manual code review.  No warranty is implied - use as it is.   

This is an attempt at a simple web application that will help rebalance a equity portfolio against a desired asset allocation target.

Key features:
- Google OAuth authentication
- Private and public security products that are tied to asset classes.  Public security price updates via yFianance.
- Securities can be restricted to specific accounts, a preference model, or unrestricted (any account)
- Multi account types: Registered and non-registered.   Future features will take advantage of the tax-exempt nature of registered accounts.
- Asset Allocation targets
- Rebalance against the targets

✅ What the Algorithm Does:
- Portfolio-Level Rebalancing: Identifies overweight and underweight asset classes across your entire portfolio
- Account-Matched Trading: Sells and buys happen in the same account to avoid contribution limit issues
- Existing Position Preference: Prefers adding to existing holdings over creating new positions in an account
- Balanced Threshold: Only rebalances asset classes that deviate more than 0.50% from target (configurable in Settings)
- Security Restrictions: Respects per-security account restrictions (configured on Securities page)
- Fractional Shares: Final trade per account uses fractional shares to minimize leftover cash

🎯 How It Chooses What to Buy:
- Find Best Account: Prioritizes accounts with existing holdings AND sellable overweight positions
- Generate Sells First: Sells overweight positions in that account to generate cash
- Generate Buys: Uses the generated cash to buy underweight positions
- Prefer Existing Securities: Adds to positions you already own when possible

🔄 Transaction Sequencing:
- Account Grouping: All sells and buys for an account are grouped together
- Sell First: Sells execute first to generate cash
- Buy Second: Buys execute using the generated cash
- No Cross-Account Transfers: Each account is self-contained

⚠️ What the Algorithm Does NOT Do (Yet):
- Cross-Account Cash Transfers: Doesn't move money between accounts (respects contribution limits)
- Tax-Loss Harvesting: Doesn't identify losing positions to offset gains
- Wash Sale Awareness: Doesn't avoid repurchasing securities sold at a loss within 30 days
- Cost Basis Tracking: Doesn't track purchase prices or holding periods for capital gains
- Long-term vs Short-term Gains: Doesn't consider holding periods for preferential tax treatment
- Specific Lot Selection: Doesn't let you choose which shares to sell (FIFO, LIFO, etc.)

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
