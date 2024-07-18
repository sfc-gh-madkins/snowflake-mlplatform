# Snowflake ML Platform: An example repo for getting models into production

Snowflakes point of view is that AI & machine learning is a superset of software engineering. The purpose of this repository is be a starting point (and includes a demo) that satisfies all of the requisite software engineering best practices for safely deploying a machine learning model into production using entirely Snowflake primitives.

From feature definitions and transforms to safely testing model-powered applications for production use cases, Snowflake makes it possible for everything to be source controlled and orchestrated as code. Lets dive in!

Much like how standard software code is safely deployed to production, we will:
1.) Create a clone of the production environment
2.) Make changes to the clone such as adding new features, datasets, or models that we want to add to the production environment
3.) Test the clone with the changes to ensure that it is safe to deploy to production (the CI part)
4.) Incrementally apply the changes that have been tested and approved from the  clone to production environment (the CD part)

*In this repository, we will use  Github actions to facilate the CI/CD process. For testing, this will happen on a new PR. For deployment, this will happen on a release. This can certainy be modified to fit your team's MLOps practices.*

From a Snowflake perspective, the key features of the pipeline includes:
- Snowflake Feature Store (the ability to define features declaratively)
- Snowflake Datasets (the ability to create point-in-time correct datasets for model training)
- Snowflake Model Registry (the ability to performance ultra-scalable inference)

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

Snowflakes point of view is that AI & machine learning is really a superset of software engineering
