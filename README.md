# Simple JSON POC

This Scala project demonstrates the processing of JSON data using the [Circe](https://circe.github.io/circe/) library. It validates JSON events against a predefined schema and extracts data based on a mapping configuration.

## Project Structure

- `src/main/scala/com/example/test4/`: Contains the Scala source code.
    - `test6.scala`: The main Scala file implementing JSON validation and data extraction.

## Prerequisites

Before running the project, ensure you have:

- Scala installed on your machine.
- JSON schema file (`inputSchema.json`) in the specified location.
- JSON event file (`event.json`) in the specified location.
- Data mapping file (`data_mapping1.json`) in the specified location.

## How to Run

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/your-repository.git
    cd your-repository
    ```

2. Run the Scala project:

    ```bash
    sbt run
    ```

## Configuration Files

- `inputSchema.json`: Defines the JSON schema for validation.
- `event.json`: Contains the JSON event data for validation.
- `data_mapping1.json`: Specifies the mapping configuration for data extraction.

## Output

The project outputs the result of JSON validation and the extracted data for both single and multiple rows.
