# Use a Python 3.12 base image
FROM python:3.12

# Set the working directory inside the container
WORKDIR /app

# Copy the application files into the working directory
COPY . /app

# Install system dependencies. Using 'default-jdk' is more robust as it will automatically
# link to the latest available JDK (likely OpenJDK 17 on this base image).
# We chain the commands to create a single layer and avoid common caching issues.
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables. The path to the default JDK is often /usr/lib/jvm/default-java.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python dependencies. Use a single RUN command for efficiency.
RUN pip install --no-cache-dir kafka-python requests pyspark==3.5.1 \
    duckdb==1.0.0 yfinance ccxt vaderSentiment prefect streamlit plotly

# Define the default command to run when the container starts
CMD ["python"]
