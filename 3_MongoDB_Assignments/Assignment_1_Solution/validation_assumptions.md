# Task 6 — Data Validation in Kafka Consumer

## Objective

Implement data validation and cleaning checks in the Kafka consumer before
ingesting logistics records into MongoDB.

The validation logic checks for:

- Null and missing values
- Data type validity
- Numeric ranges
- Date/time formats
- Mandatory and optional fields

The consumer validates and cleans each Kafka record before deciding whether
the record should be inserted into MongoDB.

---

## 1. Validation Approach

The Kafka consumer receives each Avro message as a Python dictionary.

The record follows this flow:
```
Kafka Message
      ↓
Avro Deserialization
      ↓
validate_and_clean()
      ↓
 ┌───────────────┐
 │ Validation    │
 │ + Cleaning    │
 └───────┬───────┘
         ↓
   ┌─────┴─────┐
   │           │
 Errors      No Errors
   │           │
   ↓           ↓
Reject       MongoDB
Record       Insert
               ↓
          Commit Offset
```

The validation and cleaning logic is implemented in a single function:
`validate_and_clean(record)`.

The function returns:

```python
cleaned_record, warnings, errors
```

---

# 2. Mandatory Fields

The following fields are considered mandatory:

```text
BookingID
vehicle_no
Origin_Location
Destination_Location
trip_start_date
customerID
supplierID
```

### Assumption

A missing value in any of these fields makes the record invalid.

The record is therefore rejected and is not inserted into MongoDB.

---

# 3. Optional Fields

Fields that are not part of the mandatory-field list are treated as optional.

If an optional field is missing:

- A warning is generated.
- The field is retained with `None`.
- The record is still eligible for MongoDB insertion.

Example:

```text
Driver_MobileNo: optional field is missing
```

No arbitrary default value is assigned to missing optional fields.

### Reason

Assigning artificial values could introduce incorrect information into the
dataset. Therefore, missing optional values are preserved as `None`.

---

# 4. Empty String Handling

If a field contains an empty string:

```python
""
```

it is converted to:

```python
None
```

and a warning is generated.

Example:

```text
field: empty string converted to None
```

This allows empty values to be handled consistently with other missing values.

---

# 5. Numeric Validation

The following fields are validated as numeric fields:

```text
Curr_lat
Curr_lon
TRANSPORTATION_DISTANCE_IN_KM
Minimum_kms_to_be_covered_in_a_day
```

## 5.1 Latitude

`Curr_lat` must be within the valid geographical latitude range:

```text
-90 to 90
```

If the value is numeric, it is converted to `float`.

If it is a numeric string, the consumer attempts to convert it to `float`
and generates a warning when conversion is required.

Invalid numeric values or values outside the valid range result in a
validation error.

---

## 5.2 Longitude

`Curr_lon` must be within:

```text
-180 to 180
```

The same conversion and validation approach used for latitude is applied.

Invalid numeric values or values outside the valid range result in a
validation error.

---

## 5.3 Transportation Distance

`TRANSPORTATION_DISTANCE_IN_KM` must:

- Be numeric
- Be greater than or equal to `0`

Numeric values are converted to `float`.

Negative values or invalid numeric values result in a validation error.

---

## 5.4 Minimum Daily Distance

`Minimum_kms_to_be_covered_in_a_day` must:

- Be numeric
- Be greater than or equal to `0`

Negative values or invalid numeric values result in a validation error.

If the field is missing, it is treated as an optional missing field.

---

# 6. Date and Time Validation

The following fields are validated using specific date/time formats:

| Field | Expected Format |
|---|---|
| `BookingID_Date` | `%m/%d/%y` |
| `actual_eta` | `%m/%d/%y %H:%M` |
| `trip_start_date` | `%m/%d/%y %H:%M` |
| `trip_end_date` | `%m/%d/%y %H:%M` |

Python's `datetime.strptime()` is used for validation and conversion.

For example:

```python
datetime.strptime(value, "%m/%d/%y %H:%M")
```

Valid values are converted from strings into Python `datetime` objects
before being inserted into MongoDB.

Invalid date/time values generate validation errors.

---

# 7. Fields Without Additional Format Validation

Some fields were intentionally not given additional format rules because
their business meaning or format could not be established reliably from
the provided dataset.

### BookingID

No regular-expression validation is applied to `BookingID`.

The dataset contains Booking IDs with different patterns, so imposing an
arbitrary format restriction could incorrectly reject valid records.

The field is still required because `BookingID` is a mandatory field.

### Data_Ping_time and Planned_ETA

No additional format validation is applied to these fields.

The observed values contain formats such as:

```text
40:28.0
23:40.7
```

Their exact semantic representation could not be established confidently
from the dataset, so no arbitrary validation rule was imposed.

---

# 8. String Validation

Generic string-format validation is not separately implemented.

The Avro schema already defines the expected data types for the Kafka
messages, and the producer performs the required type normalization before
publishing the records.

Therefore, the consumer focuses on business-level validation such as:

- Mandatory fields
- Numeric ranges
- Numeric conversion
- Date/time formats
- Missing values

---

# 9. Validation Errors vs Warnings

The validation process separates problems into two categories.

## Errors

Errors indicate that the record should not be inserted into MongoDB.

Examples:

- Missing mandatory field
- Invalid numeric value
- Invalid coordinate
- Negative distance
- Invalid date/time format

If errors exist:

```text
Record → Rejected
```

The Kafka offset is still committed because the record has been handled by
the consumer and intentionally rejected.

---

## Warnings

Warnings indicate that the record can still be processed.

Examples:

- Missing optional field
- Empty string converted to `None`
- Numeric value converted to `float`

If only warnings exist:

```text
Record → MongoDB
```

---

# 10. MongoDB Insertion and Kafka Offset Commit

A record with no validation errors is inserted into MongoDB.

The `BookingID` is used as the MongoDB `_id`:

```python
cleaned_record["_id"] = cleaned_record["BookingID"]
```

The Kafka offset is committed only after a successful MongoDB insertion.

The processing flow is:

```text
Kafka Message
     ↓
Validation
     ↓
Valid?
 ┌───┴────┐
 │        │
No       Yes
 │        │
Reject   MongoDB Insert
 │        │
Commit   Success?
          ┌─┴─┐
         Yes  No
          │    │
        Commit │
               │
          No Commit
```

This prevents a successfully committed Kafka offset from being recorded
before the corresponding MongoDB insertion has succeeded.

---

# 11. Summary of Validation Assumptions

| Validation Area | Assumption / Decision |
|---|---|
| Mandatory fields | BookingID, vehicle_no, Origin_Location, Destination_Location, trip_start_date, customerID, supplierID |
| Optional fields | Missing values generate warnings and remain `None` |
| Empty strings | Converted to `None` |
| Latitude | Must be between -90 and 90 |
| Longitude | Must be between -180 and 180 |
| Distance fields | Must be numeric and >= 0 |
| BookingID_Date | `%m/%d/%y` |
| actual_eta | `%m/%d/%y %H:%M` |
| trip_start_date | `%m/%d/%y %H:%M` |
| trip_end_date | `%m/%d/%y %H:%M` |
| BookingID format | No additional regex validation |
| Data_Ping_time | No additional format validation |
| Planned_ETA | No additional format validation |
| Generic string validation | Relied on Avro schema/type normalization |
| Invalid records | Rejected and offset committed |
| Valid records | Inserted into MongoDB and offset committed after successful insertion |


