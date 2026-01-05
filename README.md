# Running Apache Airflow

Follow these steps to start and access your Airflow environment:

1. **Start Airflow services**  
   Run the command below to start all Airflow services in detached mode:

   ```bash
   docker-compose up -d
   ```

2. **Access the Airflow web interface**  
   Open your browser and go to:

   ```
   http://localhost:8080
   ```

3. **Log in**  
   Use the credentials below, based on what is set in your `.env` file:

   - **Username:** `_AIRFLOW_WWW_USER_USERNAME`
   - **Password:** `_AIRFLOW_WWW_USER_PASSWORD`

# How to deploy using Traefik

1. **Navigate to the Traefik directory**  

   ```bash
   cd traefik
   ```

2. **Get your Cloudflare API key**  
   Go to [dash.cloudflare.com](https://dash.cloudflare.com) to obtain your API key.  
   This is needed because we’re using **DNS challenge** to request an SSL certificate from Let’s Encrypt.

   Select your domain and create an A record pointing your domain (e.g., airflow.devrayco.name.ng) to your server’s public IP address.

   After that, go to Profile → API Tokens → Edit Zone DNS to generate an API token that has permission to manage your domain’s DNS. This token will be used by Traefik for the DNS challenge to automatically request SSL certificates from Let’s Encrypt

3. **Start Traefik**  

   ```bash
   docker-compose up -d
   ```

4. **Check Traefik logs**  

   ```bash
   docker logs -f traefik
   ```

5. **Configure Airflow apiserver for Traefik**  
   In your `docker-compose.yml` file, add a `labels` section under the `airflow-apiserver` service.  
   This tells Traefik how to route traffic to Airflow. For example:

   ```yaml
    airflow-apiserver:
        <<: *airflow-common
        command: api-server
        ports:
        - "8080:8080"
        healthcheck:
        test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/version"]
        interval: 30s
        timeout: 10s
        retries: 5
        start_period: 30s
        labels:
        - "traefik.enable=true"
        - "traefik.http.routers.airflow.rule=Host(`airflow.devrayco.name.ng`)"
        - "traefik.http.routers.airflow.entrypoints=websecure"
        - "traefik.http.routers.airflow.tls.certresolver=myresolverv1"
        - "traefik.http.services.airflow.loadbalancer.server.port=8080"
        networks:
        - chatai
        restart: always
        depends_on:
        <<: *airflow-common-depends-on
        airflow-init:
            condition: service_completed_successfully
    ```

   Replace `airflow.devrayco.name.ng` with your actual domain.  
  