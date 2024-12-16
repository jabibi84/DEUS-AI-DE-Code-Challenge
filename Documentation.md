# Project Documentation

## Overview

This project aims to solve DEUS.AI Data Engineering Challenge. The approach focuses on keep it simple implementing a scalable solution to deliver reliable, reproducible results.

## Approach

Primary approach can be broken down into the following key steps:

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
   - **Data Preprocessing:** Load data in raw format 
   - **Validate:** Perform check data quality
   - **Cleaning:** Fix any issue detected in the previous stage.  
   - **Transformed:** Once data is cleaned and validate perform transformations requested.   
  
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
- **Granularity of changes:**
  - Granularity and Traceability:By committing small, discrete changes, it’s easier to identify which commit introduced a specific feature, bug, or improvement. This makes debugging and rolling back problematic changes more straightforward.
  - Clarity and Communication: Clear, concise commit messages tied to focused changes help others understand the purpose and logic behind each modification. This well-structured commit history can serve as a valuable reference over time.
  - Improved Code Review Process: Small commits make code reviews more manageable and less time-consuming. Reviewers can more easily spot issues, ensuring that feedback is timely and corrections are more efficient.
  - Easier Reversions: If a particular commit turns out to be problematic, reverting just that commit is simpler when it contains a minimal set of changes rather than a large, mixed batch of updates.


## Steps to Run the Code and Reproduce Results

1. **Prerequisites:**
   - **Install Docker:**  
     Ensure you have Docker installed. Follow instructions at [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/).

2**Create new python enviroment 
  
    python -m venv env

2.1. **Activate new python environment: 

Windows

    .\env\scripts\activate

MacOS & Linux
    
    source ./env/bin/activate

3. **Configure githook to run test code before each commit 

    ```bash
    cp ./git-hook/* ./.git/hooks/
    ```

4. **Install Python Packages  
    ```bash 
   pip install -r ./requirements.txt
   ```

5. **Build the Docker Compose:**
   ```bash
   docker compose up --build
    ```
Once the container el running it will submit a spark job with the code of solution, its because local folder code are mapping within container, so you can make any change in data files or code and run docker compose command and it will take those new changes during executions. 

