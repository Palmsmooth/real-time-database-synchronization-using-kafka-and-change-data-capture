# Data Engineering Project: Real-Time Database Synchronization Using Kafka and Change Data Capture

## Project Overview
This project aims to develop a system for DataTH Cineplex to synchronize data between databases using Kafka. The system will implement Change Data Capture (CDC) to capture the binlog from the source database, process the incoming data, and apply it to the destination database accurately, completely, and immediately in real-time synchronization.

<img src="image\project overview.png" width="100%" height="40%">

---

## Getting Start
### 1. Prerequisite&Tools
<img src="image\Prerequisite&Tools.png" width="100%" height="40%">

#### Step 1: Provision a Cloud SQL
#### Step 2: Provision a GCP Compute Engine instance

---

### 2. Data Flow Chart
<img src="image\Data flow chart.png" width="100%" height="40%">

#### step 1: Mysql Source Kafka
#### step 2: Mysql Sink Kafka
#### step 3: Ksqldb
##### step 3.1: Ksqlststream
##### step 3.2: Mysql Sink Ksql

---
