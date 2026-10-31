import csv, json, os
import random
from datetime import date, timedelta, datetime
from google.cloud import storage

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
date_folder = datetime.now().strftime("%Y-%m-%d")

# Configuration
BUCKET_NAME = "daily_data_assignment_1"
LOCAL_FILE = f"/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/5_Airflow_Assignments/My_Assignment_1_Solution/data/employees_{timestamp}.csv"
GCS_FILE = f"{date_folder}/employees_{timestamp}.csv"
STATE_FILE = "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/5_Airflow_Assignments/My_Assignment_1_Solution/data/employee_state.json"
NUM_EMPLOYEES = 100


def get_last_employee_id():
    if not os.path.exists(STATE_FILE):
        return 0

    with open(STATE_FILE, "r") as file:
        state = json.load(file)

    return state["last_employee_id"]

def save_last_employee_id(employee_id):
    state = {"last_employee_id": employee_id}

    with open(STATE_FILE, "w") as file:
        json.dump(state, file, indent=4)


# Sample data
first_names = [
    "Aarav", "Vivaan", "Aditya", "Arjun", "Ishaan",
    "Priya", "Ananya", "Diya", "Aanya", "Meera"
]

last_names = [
    "Sharma", "Verma", "Singh", "Gupta", "Patel",
    "Kumar", "Reddy", "Joshi", "Mehta", "Das"
]

departments = [
    "Engineering", "Data", "Finance",
    "HR", "Marketing", "Operations"
]

locations = ["Delhi", "Mumbai", "Bengaluru", "Pune", "Hyderabad"]


def generate_employees(start_id, num_employees):
    employees = []

    for employee_id in range(start_id, start_id + num_employees):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        hire_date = date(2020, 1, 1) + timedelta(
            days=random.randint(0, 2400)
        )

        employee = {
            "employee_id": employee_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": (
                f"{first_name.lower()}.{last_name.lower()}"
                f"{employee_id}@example.com"
            ),
            "department": random.choice(departments),
            "salary": random.randint(300000, 1800000),
            "location": random.choice(locations),
            "hire_date": hire_date.isoformat()
        }

        employees.append(employee)

    return employees


def save_to_csv(employees, filename):
    fieldnames = employees[0].keys()

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(employees)

    print(f"Generated {len(employees)} employees in {filename}")


def upload_to_gcs(bucket_name, local_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(
        local_file,
        content_type="text/csv"
    )

    print(f"Uploaded to gs://{bucket_name}/{destination_blob}")


def main():
    last_id = get_last_employee_id()
    start_id = last_id + 1

    employees = generate_employees(start_id, NUM_EMPLOYEES)

    save_to_csv(employees, LOCAL_FILE)

    upload_to_gcs(
        BUCKET_NAME,
        LOCAL_FILE,
        GCS_FILE
    )

    if employees:
        new_last_id = employees[-1]["employee_id"]
        save_last_employee_id(new_last_id)

    print(f"Last employee ID saved: {new_last_id}")


if __name__ == "__main__":
    main()
