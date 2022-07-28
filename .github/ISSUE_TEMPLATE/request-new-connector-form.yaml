name: Request new connector
about: Request connectivity to a system that isn't yet supported in Flow
title: Request a connector to [capture from | materialize to] [your favorite system]
labels: new connector
assignees: ''
body:
  - type: markdown
    attributes:
      value: |
        Please fill out this form with details about the system you'd like to connect to.
  - type: input
    id: system-name
    attributes:
      label: System Name
      description: Name of the system you want to connect
      placeholder: ex. AcmeCo Object Storage
    validations:
      required: true
  - type: dropdown
    id: connector-type
    attributes:
      label: Type
      description: Do you want to capture data from this system, materialize data into it, or both
      options:
        - Capture
        - Materialize
        - Both
    validations:
      required: true
  - type: textarea
    id: details
    attributes:
      label: Details
      description: Please provide whatever details you can about your use case.
      placeholder: I want to capture CSV files from AcmeCo object storage
    validations:
      required: false

