FROM mcr.microsoft.com/playwright:v1.44.0-jammy

WORKDIR /app

# Copy package files
COPY package.json ./

# Install dependencies
RUN npm install

# Install Playwright browsers
RUN npx playwright install chromium

# Copy source
ARG CACHEBUST=10
COPY index.js ./src/index.js

# Expose port
EXPOSE 3000

# Start
CMD ["node", "src/index.js"]
