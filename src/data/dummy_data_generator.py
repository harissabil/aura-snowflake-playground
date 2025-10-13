import pandas as pd
from datetime import datetime, time, timedelta
import random


class HospitalDataGenerator:
    """Generate realistic dummy data for hospital system"""

    @staticmethod
    def generate_sop_data(num_records: int = 20) -> pd.DataFrame:
        """Generate hospital SOP data"""
        categories = ["Patient Care", "Emergency Procedures", "Administrative",
                      "Safety Protocol", "Quality Assurance"]

        departments = ["Emergency", "ICU", "Outpatient", "Surgery",
                       "Radiology", "Laboratory", "Administration"]

        sop_templates = {
            "Patient Care": [
                "Patient Admission Procedure",
                "Patient Discharge Process",
                "Medication Administration Protocol",
                "Patient Transfer Guidelines"
            ],
            "Emergency Procedures": [
                "Code Blue Response",
                "Fire Emergency Protocol",
                "Mass Casualty Incident Response",
                "Medical Emergency Response"
            ],
            "Administrative": [
                "Medical Record Management",
                "Appointment Scheduling Procedure",
                "Insurance Verification Process",
                "Patient Registration Protocol"
            ],
            "Safety Protocol": [
                "Infection Control Measures",
                "Hand Hygiene Protocol",
                "Personal Protective Equipment Usage",
                "Waste Disposal Guidelines"
            ],
            "Quality Assurance": [
                "Incident Reporting Procedure",
                "Quality Audit Process",
                "Patient Feedback Management",
                "Clinical Documentation Standards"
            ]
        }

        data = []
        for i in range(num_records):
            category = random.choice(categories)
            title = random.choice(sop_templates[category])

            data.append({
                "SOP_ID": f"SOP-{i + 1:04d}",
                "SOP_CATEGORY": category,
                "SOP_TITLE": title,
                "SOP_CONTENT": f"This is the detailed procedure for {title}. "
                               f"Step 1: Assess the situation. "
                               f"Step 2: Follow department guidelines. "
                               f"Step 3: Document all actions taken. "
                               f"Step 4: Report to supervisor if necessary.",
                "DEPARTMENT": random.choice(departments),
                "LAST_UPDATED": datetime.now() - timedelta(days=random.randint(1, 365)),
                "VERSION": f"v{random.randint(1, 5)}.{random.randint(0, 9)}"
            })

        return pd.DataFrame(data)

    @staticmethod
    def generate_doctor_schedule(num_doctors: int = 15) -> pd.DataFrame:
        """Generate doctor schedule data"""
        specializations = [
            "General Practitioner", "Cardiologist", "Pediatrician",
            "Orthopedist", "Dermatologist", "Neurologist",
            "Gynecologist", "Psychiatrist", "ENT Specialist"
        ]

        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

        data = []
        schedule_id = 1

        for doc_id in range(1, num_doctors + 1):
            doctor_name = f"Dr. {random.choice(['Ahmad', 'Budi', 'Citra', 'Dewi', 'Eko', 'Fitri', 'Gita', 'Hadi'])} {random.choice(['Santoso', 'Wijaya', 'Kusuma', 'Pratama', 'Sari', 'Permata'])}"
            specialization = random.choice(specializations)

            # Generate 2-4 schedule slots per doctor
            num_slots = random.randint(2, 4)
            selected_days = random.sample(days, num_slots)

            for day in selected_days:
                start_hour = random.choice([8, 9, 10, 13, 14, 15])
                end_hour = start_hour + random.choice([2, 3, 4])

                data.append({
                    "SCHEDULE_ID": f"SCH-{schedule_id:04d}",
                    "DOCTOR_ID": f"DOC-{doc_id:03d}",
                    "DOCTOR_NAME": doctor_name,
                    "SPECIALIZATION": specialization,
                    "DAY_OF_WEEK": day,
                    "START_TIME": time(start_hour, 0),
                    "END_TIME": time(end_hour, 0),
                    "ROOM_NUMBER": f"{random.randint(1, 5)}{random.randint(0, 9):02d}",
                    "MAX_PATIENTS": random.choice([10, 15, 20]),
                    "BOOKED_PATIENTS": random.randint(0, 15),
                    "STATUS": random.choice(["AVAILABLE", "AVAILABLE", "AVAILABLE", "FULL"])
                })
                schedule_id += 1

        return pd.DataFrame(data)

    @staticmethod
    def generate_facility_data(num_facilities: int = 25) -> pd.DataFrame:
        """Generate hospital facility data"""
        facility_types = [
            "Operating Room", "ICU Bed", "Emergency Room", "X-Ray Room",
            "MRI Scanner", "CT Scanner", "Laboratory", "Pharmacy",
            "Consultation Room", "Waiting Area", "Blood Bank", "Cafeteria"
        ]

        locations = [
            "Building A - Floor 1", "Building A - Floor 2", "Building A - Floor 3",
            "Building B - Floor 1", "Building B - Floor 2", "Building C - Floor 1"
        ]

        data = []
        for i in range(num_facilities):
            facility_type = random.choice(facility_types)

            data.append({
                "FACILITY_ID": f"FAC-{i + 1:04d}",
                "FACILITY_NAME": f"{facility_type} {random.randint(1, 5)}",
                "FACILITY_TYPE": facility_type,
                "LOCATION": random.choice(locations),
                "CAPACITY": random.choice([1, 2, 5, 10, 20, 50]),
                "CURRENT_USAGE": random.randint(0, 10),
                "OPERATING_HOURS": random.choice(["24/7", "08:00-17:00", "08:00-20:00"]),
                "CONTACT_INFO": f"ext. {random.randint(1000, 9999)}",
                "EQUIPMENT_LIST": "Standard medical equipment, monitors, emergency supplies",
                "STATUS": random.choice(["OPERATIONAL", "OPERATIONAL", "OPERATIONAL", "MAINTENANCE"])
            })

        return pd.DataFrame(data)
