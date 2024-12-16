# Project Documentation

## Overview

This project aims to solve DEUS.AI Data Engineering Challenge. My approach focuses on keep it simple implementing a scalable solution to deliver reliable, reproducible results.

## Approach

My primary approach can be broken down into the following key steps:

1. **Problem Analysis:**  
   Begin analyzing the domain and identifying the core challenges. This included:
   - Understanding the data sources and their formats.
   - Defining the primary objective.
   - Design alternatives.

2. **Solution Design:**  
   Based on the analysis, created components solution:
   - **Architecture:**  
     I adopted a modular architecture to maintain flexibility and scalability. The solution is composed of discrete components (e.g., Proccesing, Validation, Cleaning and Transformation).
  ```bash
    root/
    ├─ data/
    │  ├─ input/
    │  └─ output/
    │
    ├─ src/
    │  ├─ proccesing.py
    │  ├─ validation.py
    │  ├─ cleanning.py
    │  ├─ transformations.py
    │  └─ utils.py
    │
    ├─ configs/
    │  └─ metadata.yaml
    │
    ├─ tests/
    │  ├─ test_data_preprocessing.py
    │  ├─ test_train_model.py
    │  └─ test_app.py
    │
    ├─ Dockerfile
    ├─ dcoker-compose.yml
    ├─ requirements.txt
    ├─ README.md
    └─ .github/
       └─ workflows/
          └─ main.yaml
   ```   

  - **Technology Stack:**  
    I selected tools aligned with the problem’s requirements. For instance:
    - **Language:** Python 3.10+ for its rich ecosystem.
    - **Infrastructure:** Docker containers & Compose for reproducible environments.

3. **Implementation Details:**  
   Each component is implemented with clarity and extensibility in mind:
   - **Data Preprocessing:** Load, Validate, cleaned and transformed.  
  
4. **Validation & Testing:**  
   Comprehensive testing ensures reliability:
   - **Unit Tests:** Validate individual modules.  
   - **Integration Tests:** Verify that components work together as intended.  
   - **Performance Tests:** Assess speed and resource usage.  
   - **User Acceptance Tests (If Applicable):** Confirm that the solution meets end-user requirements.

5. **Deployment:**  
   The final solution is packaged into a Docker container for ease of deployment. Continuous Integration/Continuous Deployment (CI/CD) pipelines help maintain code quality and streamline updates.

## Assumptions and Decisions

- **Data Quality Assumption:**  
   assume the input data is not sufficient quality (e.g., severe missing values or corrupted files) or can be made so through preprocessing.
- **Resource Constraints:**  
  prioritize efficiency over absolute accuracy if the latter required significantly higher computational resources. This ensures that the solution runs within reasonable time frames and hardware limits.
- **Technology Decisions:**  
  Using Python and Docker was guided by the availability of community support, existing tools, and the ease of integration.

## Steps to Run the Code and Reproduce Results

1. **Prerequisites:**
   - **Install Docker:**  
     Ensure you have Docker installed. Follow instructions at [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/).

2. **Build the Docker Image:**
   ```bash
   docker compose up --build

4. **Create Python Environment to run local Plint
    ```bash
   python3 -m venv 

5. **Activate Python Environment
    ```bash 
   source ./env/bin/activate

6. **Install Python Packages  
    ```bash 
   pip install -r ./requirements.txt
   ```
