# Airflow EC2 Setup Guide

This guide describes how to launch an EC2 instance named `airflow`, connect to it, and prepare the environment for running Apache Airflow using Docker.

---

## 🖥️ 1. Launch EC2 Instance

### AWS Console Setup
    
1. Go to the **EC2 Dashboard** on the AWS Console.
2. Click **Launch Instance**.           
3. Configure the instance as follows:
   - **Name:** `airflow`
   - **AMI:** Ubuntu Server 22.04 LTS (or similar)
   - **Instance Type:** `t2.micro` or higher
   - **Key Pair:** Create or select an existing key pair
   - **Security Group:**
     - Allow **SSH (port 22)**
     - (Optional) Allow port **8080** for Airflow Web UI
4. Launch the instance.

-

## 🔐 2. Connect to the Instance

Replace `<key-pair-file>` and `<public-dns>` with your values.

```bash
ssh -i <key-pair-file>.pem ubuntu@<your-ec2-public-dns>
