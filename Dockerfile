# Use the official Playwright image — it has Chrome + all Linux dependencies pre-installed
FROM mcr.microsoft.com/playwright:v1.51.0-jammy

# Set working directory inside the container
WORKDIR /app

# Copy package files first (Docker caches this layer — speeds up rebuilds)
COPY package*.json ./

# Install Node dependencies
RUN npm install

# Install Playwright's Chromium browser
RUN npx playwright install chromium

# Install concurrently during build
RUN npm install concurrently


# Copy the rest of your project files
COPY . .

# Expose the API and bot ports
EXPOSE 6060
EXPOSE 7070
EXPOSE 10000
# Default command — starts the API server
# Render will override this for the worker (bot) service via render.yaml
#CMD ["node", "api-server.js"]

# Run both services
#CMD ["npx", "concurrently", "node index.js", "node api-server.js"]
CMD ["node", "start.js"]