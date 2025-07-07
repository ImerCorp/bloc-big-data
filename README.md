# Server Setup Guide for Airflow

This guide describes how to launch a server instance named `airflow`, connect to it, 
and prepare the environment for running Apache Airflow using Docker. 
I used Hetzner and EC2 as well to host my solution. The purpose of this repository 
is being forked to build light ELT solutions using Airflow.

---

## 1. Launch Server Instance

### Server Console Setup
   
1. Go to your **Server Dashboard** on the hosting console.
2. Click **Launch Instance**.          
3. Configure the instance as follows:
   - **Name:** `airflow`
   - **OS:** Ubuntu Server 22.04 LTS (or similar)
   - **Key Pair:** Create (keygen) or select an existing key pair
   - **Security Group:**
     - Allow **SSH (port 22)**
     - (Optional) Allow port **8080** for Airflow Web UI
4. Launch the instance & connect via ssh.
5. Install Docker & docker-compose on the instance.

```bash
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
apt install docker-compose
```

---

## 2. Connect to the Instance
A little breakdown on how to connect to the console from your device.
```bash
sudo ssh -i <private-key-file-name-path> root@<ip-address>
```

## 3. Configure the env file

The env file is built from ".github/workflows/airflow_env.yml", it copy paste the secrets
to provide a configurable environment from Github. The dags, the docker files can then use this env file. Here's how you can do it :
- Add a secret.
- Add a line to the cat output in ".github/workflows/airflow_env.yml".
  - e.g : AIRFLOW_UID=${{ secrets.AIRFLOW_UID }}.
