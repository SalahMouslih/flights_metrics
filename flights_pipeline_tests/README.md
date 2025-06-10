# flights_pipeline_tests

This folder contains automated tests for the flight data pipeline project.

## Contents

- **Dagster Asset Tests**  
  Tests focused on verifying the correctness, data quality, and expected outputs of Dagster assets. These tests ensure that each transformation step produces the expected results and that the pipeline logic is sound.


Asset dependency correctness and partial rerun scenarios
- **API Client Tests**  
  Pytest-based tests for the API client modules that interact with the flights APIs. These tests validate the integration points and data fetching correctness.


**Test Coverage** *(to be extended)* 

- Schema validation

- Data quality checks (nulls, ranges, formats)

- API client response handling
## Running Tests

## Running Tests

Before running the tests, make sure you have installed the required Python dependencies listed in `requirements.txt`:

```
pip install -r requirements.txt
```

To run unit tests in this folder, execute the following command from the project root:

```
pytest flights_pipeline_tests/
```

