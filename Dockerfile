# Use the official TimescaleDB image with PostgreSQL 16 as the base
FROM timescale/timescaledb:latest-pg16

# Install Node.js 18 and necessary tools
RUN apk upgrade && apk add  \
    curl \
    nodejs \
    npm \
    postgresql16 \
    postgresql16-dev \
    postgresql16-contrib 

# Set working directory for the Express app
WORKDIR /app

# Copy package.json and install Node.js dependencies
COPY package*.json ./
RUN npm install

# Copy the rest of the application code
COPY . .

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=mysecretpassword
ENV POSTGRES_DB=stock_data

# Set environment variables for the app to connect to the DB
ENV DB_HOST=localhost
ENV DB_USER=postgres
ENV DB_PASSWORD=mysecretpassword
ENV DB_NAME=stock_data
ENV DB_PORT=5432

# Expose ports (5432 for PostgreSQL, 3000 for Express app)
EXPOSE 5432 3000

# Make the start script executable
RUN chmod +x /app/start.sh

# # Use the start script to launch both services
CMD ["/app/start.sh"]