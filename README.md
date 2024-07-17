# Example of a CI/CD pipeline for Snowflake ML Platform

This repo provides an example of a CI/CD pipeline for the Snowflake ML Platform. The pipeline is implemented using GitHub Actions and demonstrates how to automate the deployment of a machine learning model to Snowflake.
	
Some of the key features of the pipeline include:  
- Feature Store (can be installed if you don't have it)
- Training an ML Model (part of the integration test)

Tests  
- Unit Tests
- Integration Tests

The demo shows a CI/CD approach using Github actions. Most teams will modify this to reflect their own MLOps practices. This example is to show possibilities, not recommend a specific workflow. 

---

## To get this demo running:
A. Fork the MLPlatform Repo: https://github.com/sfc-gh-madkins/snowflake-mlplatform

B. Click on the Actions tab and Select that you understand workflows will be enabled

C. Add a new Environment for testing under Settings - name it: `test`

D. Add in Environment Secrets to `test` with your values:

    SNOWFLAKE_ACCOUNT 
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    
E. Add in Environment Variables to `test`:
    
    SNOWFLAKE_DATABASE_PROD - (location of existing airline feature store)
    SNOWFLAKE_SCHEMA_PROD -  Schema for feature store - typically “FEATURE_STORE”

F. Add a new Environment for testing under Settings - name it: `prod`

H. Add in Environment Secrets to `prod` with your values:

    SNOWFLAKE_ACCOUNT 
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    
I. Add in Environment Variables to `prod`:
    
    SNOWFLAKE_DATABASE_PROD - (location of existing airline feature store)
    SNOWFLAKE_SCHEMA_PROD -  Schema for feature store - typically “FEATURE_STORE”


There is an install script if you already haven't setup the Airlines feature store. You can trigger workflows with a push or using the manual trigger 

---

## Future work:
- canary testing what does this look like?
- how does monitoring work?
- how does data quality montiors work?