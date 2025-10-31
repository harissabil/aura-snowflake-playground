import pandas as pd
from datetime import datetime, time, timedelta, date
import random
from typing import List, Dict


class EnhancedHospitalDataGenerator:
    """Generate realistic and interconnected dummy data for hospital system"""

    def __init__(self, seed: int = 42):
        """Initialize with seed for reproducibility"""
        random.seed(seed)
        self.generated_schedules = []
        self.generated_doctors = {}

    @staticmethod
    def generate_sop_data(num_records: int = 50) -> pd.DataFrame:
        """Generate comprehensive hospital SOP data"""

        sop_database = {
            "Patient Care": {
                "Emergency": [
                    ("Triage Assessment Protocol",
                     "Perform systematic patient assessment using ESI triage system. Prioritize based on acuity level. Document vital signs, chief complaint, and pain scale. Assign color-coded priority: Red (immediate), Yellow (urgent), Green (non-urgent). Notify attending physician within designated timeframe based on acuity."),
                    ("Patient Admission Process",
                     "Verify patient identity using two identifiers. Complete admission assessment within 1 hour. Obtain medical history, allergies, current medications. Assign bed based on acuity and specialty. Input all data into hospital information system. Provide patient with orientation to unit and call button instructions."),
                    ("Medication Administration Guidelines",
                     "Follow five rights: right patient, medication, dose, route, time. Verify orders electronically. Check for drug interactions and allergies. Document administration immediately. Monitor for adverse reactions within 30 minutes. Report any discrepancies to prescribing physician immediately."),
                    ("Patient Transfer Between Units",
                     "Obtain transfer order from physician. Ensure accepting unit has bed available. Complete transfer checklist including current medications, treatments, and recent vital signs. Provide verbal handoff to receiving nurse. Update patient location in system within 15 minutes."),
                    ("Patient Discharge Planning",
                     "Initiate discharge planning within 24 hours of admission. Coordinate with interdisciplinary team. Ensure prescriptions sent to pharmacy. Schedule follow-up appointments. Provide written discharge instructions. Confirm patient transportation arrangements. Complete discharge summary in EMR.")
                ],
                "ICU": [
                    ("Critical Patient Monitoring",
                     "Monitor vital signs continuously via bedside monitors. Document hemodynamic parameters hourly. Assess neurological status using GCS every 2 hours. Check ventilator settings and ABG results. Titrate vasoactive medications per protocol. Notify physician of significant changes immediately."),
                    ("Ventilator Management Protocol",
                     "Verify ventilator settings match physician orders. Monitor tidal volume, respiratory rate, PEEP, FiO2. Assess patient-ventilator synchrony. Perform endotracheal suctioning as needed using sterile technique. Document ventilator parameters every 2 hours. Collaborate with respiratory therapy for weaning protocols."),
                    ("Central Line Care and Maintenance",
                     "Assess insertion site daily for signs of infection. Change dressing per facility protocol using sterile technique. Flush lumens with saline before and after medication administration. Document line placement and patency. Remove line promptly when no longer indicated to reduce infection risk.")
                ],
                "Outpatient": [
                    ("Appointment Check-in Procedure",
                     "Greet patient and verify identity. Confirm appointment in scheduling system. Update demographics and insurance information. Collect co-payment if applicable. Provide estimated wait time. Direct patient to appropriate waiting area. Flag urgent concerns to clinical staff."),
                    ("Vital Signs Documentation",
                     "Measure blood pressure, pulse, temperature, respiratory rate, oxygen saturation, height, and weight. Document pain level using numerical scale. Record in EMR immediately. Alert nurse to abnormal values. Ensure equipment calibration is current."),
                    ("Patient Education and Counseling",
                     "Assess patient's understanding of condition and treatment plan. Provide written materials at appropriate literacy level. Demonstrate procedures or medication administration. Encourage questions and address concerns. Document education provided and patient comprehension.")
                ],
                "Surgery": [
                    ("Pre-operative Patient Preparation",
                     "Verify surgical consent and procedure site marking. Confirm NPO status and last oral intake. Review allergies and current medications. Administer pre-operative antibiotics within 60 minutes of incision. Complete surgical safety checklist. Transport patient to OR with all documentation."),
                    ("Surgical Count Procedure",
                     "Conduct initial count of all sponges, sharps, and instruments before incision. Perform additional counts before cavity closure and at skin closure. Resolve discrepancies immediately with X-ray if needed. Document all counts in operative record. Both scrub tech and circulating nurse must verify."),
                    ("Post-operative Recovery Protocol",
                     "Monitor vital signs every 15 minutes until stable. Assess pain level and administer analgesia as ordered. Check surgical site and dressings. Monitor for complications: bleeding, respiratory depression, hypothermia. Discharge to floor when PACU discharge criteria met.")
                ]
            },
            "Emergency Procedures": {
                "Emergency": [
                    ("Code Blue - Cardiac Arrest Response",
                     "Activate code blue immediately. Begin CPR with high-quality compressions at 100-120/min. Apply defibrillator pads and analyze rhythm. Follow ACLS algorithms. Assign roles: compressor, airway, medications, recorder, team leader. Rotate compressors every 2 minutes. Document all interventions with timestamps."),
                    ("Trauma Activation Protocol",
                     "Activate trauma team for qualifying criteria. Prepare trauma bay with airway equipment, IV access supplies, blood products. Perform primary survey: ABCDE approach. Obtain portable X-rays. Coordinate with radiology for CT scans. Notify OR if surgical intervention likely."),
                    ("Stroke Alert Protocol",
                     "Note exact time of symptom onset. Perform NIH Stroke Scale assessment. Obtain stat CT head without contrast. Check blood glucose and coagulation studies. Consult neurology within 15 minutes. Determine tPA eligibility if ischemic stroke. Time is brain - minimize door-to-needle time."),
                    ("Mass Casualty Incident Response",
                     "Activate hospital incident command system. Establish triage area at hospital entrance. Use START triage method: Simple Triage And Rapid Treatment. Set up decontamination area if needed. Designate treatment areas by acuity. Recall off-duty staff as needed. Document all activities.")
                ],
                "Hospital-Wide": [
                    ("Fire Emergency Evacuation",
                     "Follow RACE protocol: Rescue patients in immediate danger, Activate fire alarm, Contain fire by closing doors, Evacuate if necessary. Know evacuation routes and assembly points. Assist mobility-impaired patients first. Use stairwells, never elevators. Account for all patients and staff at assembly point."),
                    ("Hazardous Material Exposure",
                     "Isolate affected area immediately. Remove contaminated clothing if safe to do so. Decontaminate with copious water irrigation for 15-20 minutes. Don appropriate PPE before patient contact. Notify environmental health and safety. Identify substance using SDS. Treat symptomatically and provide supportive care."),
                    ("Infant/Child Abduction Response",
                     "Activate Code Pink immediately. Obtain description of infant and suspected abductor. Lock down all hospital exits. Search assigned areas systematically. Check all bags and bundles leaving facility. Notify security and local law enforcement. Review surveillance footage. Do not lift lockdown until infant recovered.")
                ]
            },
            "Administrative": {
                "Administration": [
                    ("Medical Record Documentation Standards",
                     "Use black ink for paper records. Date and time all entries. Include legible signature with credentials. Never use abbreviations from 'Do Not Use' list. Correct errors with single line, date, initial. Complete documentation within 24 hours. Ensure HIPAA compliance. Use only approved templates."),
                    ("Insurance Verification and Authorization",
                     "Verify insurance eligibility within 24 hours of admission. Obtain prior authorization for planned procedures. Check coverage limits and out-of-network status. Document insurance details in billing system. Notify patient of potential out-of-pocket costs. Submit authorization requests with clinical documentation."),
                    ("Appointment Scheduling Optimization",
                     "Schedule return patients first for continuity. Allow buffer time for new patients. Block time for administrative tasks. Confirm appointments 48 hours in advance. Maintain waiting list for cancellations. Track no-show rates by provider. Optimize schedule to minimize patient wait times."),
                    ("Patient Registration and Identity Management",
                     "Collect two forms of identification. Verify demographics including address, phone, emergency contact. Photograph patient for EMR if consented. Assign medical record number. Check for existing records to avoid duplicates. Provide privacy notice and obtain required consents.")
                ],
                "All Departments": [
                    ("Informed Consent Process",
                     "Explain procedure in terms patient understands. Discuss risks, benefits, alternatives. Answer all patient questions. Ensure consent form signed before procedure. Verify patient competent to consent or obtain surrogate decision-maker. Document conversation in medical record. Patient may withdraw consent at any time."),
                    ("Patient Rights and Responsibilities",
                     "Inform patients of right to refuse treatment, privacy, access to records, complaint process. Post Patient Bill of Rights in visible location. Address language barriers with interpreter services. Respect cultural and religious preferences. Handle complaints promptly and escalate to patient advocate if needed.")
                ]
            },
            "Safety Protocol": {
                "Hospital-Wide": [
                    ("Hand Hygiene Compliance Protocol",
                     "Perform hand hygiene before patient contact, before aseptic procedure, after body fluid exposure, after patient contact, after touching patient surroundings. Use alcohol-based hand rub or soap and water. Lather for minimum 20 seconds. Ensure hands visibly clean. Dry completely before donning gloves."),
                    ("Personal Protective Equipment Usage",
                     "Select PPE based on anticipated exposure: gloves for contact, gown for splashes, mask for droplets, N95 for airborne. Don PPE before entering patient area. Doff carefully to avoid self-contamination. Dispose in designated waste container. Perform hand hygiene after PPE removal."),
                    ("Isolation Precautions Implementation",
                     "Identify isolation category: contact, droplet, or airborne. Post isolation signage on door. Ensure appropriate PPE available outside room. Limit patient transport. Use dedicated equipment when possible. Discontinue isolation per physician order based on clinical criteria."),
                    ("Needle Stick Injury Prevention",
                     "Never recap needles. Use safety-engineered devices. Dispose sharps immediately in puncture-resistant container. Do not overfill sharps containers. Report exposures immediately. Seek medical evaluation within 2 hours. Complete incident report and follow post-exposure prophylaxis protocol."),
                    ("Fall Prevention Strategy",
                     "Complete fall risk assessment on admission and daily. Implement interventions based on risk level: bed alarm, non-slip socks, frequent toileting. Keep call light within reach. Ensure adequate lighting. Clear walkways of clutter. Educate patient and family. Document all interventions.")
                ],
                "Laboratory": [
                    ("Specimen Collection and Handling",
                     "Verify patient identity with two identifiers before collection. Use proper collection technique for specimen type. Label specimens at bedside immediately. Maintain chain of custody for forensic specimens. Store at appropriate temperature. Transport within specified timeframe. Reject improperly labeled or contaminated specimens."),
                    ("Biohazard Waste Management",
                     "Segregate waste into appropriate categories: biohazard, sharps, pharmaceutical, general. Use red bags for infectious waste. Close bags when 3/4 full. Store in designated area until pickup. Never compact biohazard waste. Train all staff on proper disposal. Maintain disposal logs.")
                ]
            },
            "Quality Assurance": {
                "All Departments": [
                    ("Incident Reporting and Analysis",
                     "Report all incidents, near misses, and hazardous conditions within 24 hours. Use non-punitive reporting system. Include objective facts without blame. Classify by severity level. Investigate root causes using systematic analysis. Implement corrective actions. Track trends to identify systemic issues."),
                    ("Medication Error Prevention",
                     "Use barcode scanning for medication administration. Perform independent double-checks for high-alert medications. Minimize interruptions during medication preparation. Use tall man lettering for look-alike drugs. Separate sound-alike medications. Standardize concentrations and dosing units. Report all errors and near misses."),
                    ("Patient Safety Rounds",
                     "Conduct multidisciplinary rounds weekly. Use structured checklist covering safety domains. Interview patients about safety concerns. Inspect environment for hazards. Review safety metrics and recent incidents. Identify good practices to share. Document findings and action items."),
                    ("Clinical Quality Indicator Monitoring",
                     "Track core measures: sepsis mortality, central line infections, surgical site infections, readmission rates, patient satisfaction. Collect data according to standard definitions. Benchmark against national standards. Report monthly to quality committee. Implement improvement initiatives for below-target metrics."),
                    ("Peer Review Process",
                     "Conduct reviews of clinical care for adverse outcomes. Use objective criteria and evidence-based standards. Maintain confidentiality per peer review protection laws. Focus on system improvements not individual blame. Provide feedback to practitioners. Track patterns requiring intervention."),
                    ("Patient Complaint Resolution",
                     "Acknowledge complaint within 24 hours. Conduct thorough investigation. Interview staff and review records. Provide written response within 7 days. Identify service recovery opportunities. Track complaint themes. Implement process improvements to prevent recurrence.")
                ]
            }
        }

        data = []
        sop_id = 1

        for category, departments in sop_database.items():
            for department, sops in departments.items():
                for sop_title, sop_content in sops:
                    version_major = random.randint(1, 4)
                    version_minor = random.randint(0, 9)
                    days_old = random.randint(30, 730)

                    data.append({
                        "SOP_ID": f"SOP-{sop_id:04d}",
                        "SOP_CATEGORY": category,
                        "SOP_TITLE": sop_title,
                        "SOP_CONTENT": sop_content,
                        "DEPARTMENT": department,
                        "LAST_UPDATED": datetime.now() - timedelta(days=days_old),
                        "VERSION": f"v{version_major}.{version_minor}"
                    })
                    sop_id += 1

                    if sop_id > num_records:
                        break
            if sop_id > num_records:
                break

        return pd.DataFrame(data[:num_records])

    def generate_doctor_schedule(self, num_doctors: int = 25) -> pd.DataFrame:
        """Generate realistic doctor schedule with proper distribution"""

        indonesian_names = [
            ("Dr. Ahmad", "Santoso"), ("Dr. Budi", "Wijaya"), ("Dr. Citra", "Kusuma"),
            ("Dr. Dewi", "Pratama"), ("Dr. Eko", "Sari"), ("Dr. Fitri", "Permata"),
            ("Dr. Gita", "Handoko"), ("Dr. Hadi", "Nugroho"), ("Dr. Indah", "Lestari"),
            ("Dr. Joko", "Susanto"), ("Dr. Kartika", "Maharani"), ("Dr. Lina", "Wulandari"),
            ("Dr. Made", "Suryanto"), ("Dr. Nina", "Puspita"), ("Dr. Oscar", "Hakim"),
            ("Dr. Putri", "Anggraini"), ("Dr. Rendi", "Firmansyah"), ("Dr. Siti", "Rahmawati"),
            ("Dr. Toni", "Setiawan"), ("Dr. Umar", "Dharmawan"), ("Dr. Vina", "Melati"),
            ("Dr. Wawan", "Kurniawan"), ("Dr. Yuni", "Safitri"), ("Dr. Zainal", "Arifin"),
            ("Dr. Ayu", "Damayanti")
        ]

        specializations_config = {
            "General Practitioner": {"slots": 4, "max_patients": 20, "common_days": 5},
            "Cardiologist": {"slots": 3, "max_patients": 15, "common_days": 4},
            "Pediatrician": {"slots": 4, "max_patients": 18, "common_days": 5},
            "Orthopedist": {"slots": 3, "max_patients": 12, "common_days": 4},
            "Dermatologist": {"slots": 3, "max_patients": 16, "common_days": 4},
            "Neurologist": {"slots": 3, "max_patients": 12, "common_days": 3},
            "Gynecologist": {"slots": 3, "max_patients": 14, "common_days": 4},
            "Psychiatrist": {"slots": 2, "max_patients": 10, "common_days": 3},
            "ENT Specialist": {"slots": 3, "max_patients": 15, "common_days": 4},
            "Ophthalmologist": {"slots": 3, "max_patients": 16, "common_days": 4},
            "Pulmonologist": {"slots": 2, "max_patients": 12, "common_days": 3},
            "Gastroenterologist": {"slots": 2, "max_patients": 10, "common_days": 3},
            "Urologist": {"slots": 2, "max_patients": 10, "common_days": 3}
        }

        days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

        data = []
        schedule_id = 1

        specialization_list = list(specializations_config.keys())

        for doc_id in range(1, num_doctors + 1):
            first_name, last_name = random.choice(indonesian_names)
            doctor_name = f"{first_name} {last_name}"
            specialization = specialization_list[(doc_id - 1) % len(specialization_list)]

            config = specializations_config[specialization]
            num_slots = config["slots"]
            max_patients = config["max_patients"]
            num_days = min(config["common_days"], 6)

            # Select consistent days for this doctor
            selected_days = random.sample(days_of_week, num_days)
            selected_days.sort(key=lambda x: days_of_week.index(x))

            # Assign time slots
            for day in selected_days:
                # Morning shift (08:00-12:00) or afternoon shift (13:00-17:00)
                if random.random() < 0.6:  # 60% morning shift
                    start_hour = random.choice([8, 9])
                    end_hour = start_hour + random.choice([3, 4])
                else:  # 40% afternoon shift
                    start_hour = random.choice([13, 14])
                    end_hour = start_hour + random.choice([3, 4])

                booked = random.randint(0, max_patients)
                status = "FULL" if booked >= max_patients else "AVAILABLE"

                data.append({
                    "SCHEDULE_ID": f"SCH-{schedule_id:04d}",
                    "DOCTOR_ID": f"DOC-{doc_id:03d}",
                    "DOCTOR_NAME": doctor_name,
                    "SPECIALIZATION": specialization,
                    "DAY_OF_WEEK": day,
                    "START_TIME": time(start_hour, 0),
                    "END_TIME": time(min(end_hour, 18), 0),
                    "ROOM_NUMBER": f"{random.randint(1, 5)}{random.randint(0, 9):02d}",
                    "MAX_PATIENTS": max_patients,
                    "BOOKED_PATIENTS": booked,
                    "STATUS": status
                })

                # Store for appointment generation
                self.generated_schedules.append({
                    "schedule_id": f"SCH-{schedule_id:04d}",
                    "doctor_id": f"DOC-{doc_id:03d}",
                    "day": day,
                    "max_patients": max_patients,
                    "booked": booked
                })

                schedule_id += 1

            # Store doctor info
            self.generated_doctors[f"DOC-{doc_id:03d}"] = {
                "name": doctor_name,
                "specialization": specialization
            }

        return pd.DataFrame(data)

    @staticmethod
    def generate_facility_data(num_facilities: int = 40) -> pd.DataFrame:
        """Generate comprehensive hospital facility data"""

        facilities_config = {
            "Operating Room": {
                "count": 8,
                "locations": ["Building B - Floor 2", "Building B - Floor 3"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "Anesthesia machine, surgical lights, patient monitors, electrosurgical unit, surgical instruments, sterilization equipment"
            },
            "ICU Bed": {
                "count": 12,
                "locations": ["Building A - Floor 3"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "Ventilator, cardiac monitor, infusion pumps, defibrillator, bedside ultrasound"
            },
            "Emergency Room": {
                "count": 6,
                "locations": ["Building A - Floor 1"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "Crash cart, patient monitor, IV pumps, oxygen supply, suction equipment, trauma supplies"
            },
            "Consultation Room": {
                "count": 20,
                "locations": ["Building C - Floor 1", "Building C - Floor 2", "Building C - Floor 3"],
                "capacity": 1,
                "hours": "08:00-17:00",
                "equipment": "Examination table, blood pressure monitor, stethoscope, otoscope, thermometer, computer workstation"
            },
            "X-Ray Room": {
                "count": 4,
                "locations": ["Building A - Floor 2"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "Digital X-ray machine, lead aprons, positioning aids, PACS workstation"
            },
            "MRI Scanner": {
                "count": 2,
                "locations": ["Building A - Floor 2"],
                "capacity": 1,
                "hours": "08:00-20:00",
                "equipment": "1.5T MRI machine, patient monitoring system, contrast injector, screening equipment"
            },
            "CT Scanner": {
                "count": 2,
                "locations": ["Building A - Floor 2"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "64-slice CT scanner, contrast injector, emergency drugs, PACS workstation"
            },
            "Laboratory": {
                "count": 3,
                "locations": ["Building A - Floor 1"],
                "capacity": 10,
                "hours": "24/7",
                "equipment": "Automated analyzers, centrifuges, microscopes, refrigeration units, safety cabinets"
            },
            "Pharmacy": {
                "count": 2,
                "locations": ["Building A - Floor 1", "Building B - Floor 1"],
                "capacity": 5,
                "hours": "24/7",
                "equipment": "Automated dispensing system, refrigeration units, computer terminals, medication storage"
            },
            "Blood Bank": {
                "count": 1,
                "locations": ["Building A - Floor 1"],
                "capacity": 200,
                "hours": "24/7",
                "equipment": "Blood refrigerators, plasma freezers, centrifuges, blood warmers, crossmatching equipment"
            },
            "Recovery Room": {
                "count": 10,
                "locations": ["Building B - Floor 2"],
                "capacity": 1,
                "hours": "24/7",
                "equipment": "Patient monitors, oxygen supply, suction equipment, warming devices, emergency medications"
            },
            "Dialysis Unit": {
                "count": 8,
                "locations": ["Building C - Floor 2"],
                "capacity": 1,
                "hours": "06:00-22:00",
                "equipment": "Hemodialysis machine, water treatment system, patient chairs, monitors, emergency supplies"
            }
        }

        data = []
        facility_id = 1

        for facility_type, config in facilities_config.items():
            for i in range(config["count"]):
                location = random.choice(config["locations"])
                capacity = config["capacity"]
                current_usage = random.randint(0, min(capacity, capacity))

                # Realistic status distribution
                status_weights = [0.85, 0.10, 0.05]  # operational, maintenance, offline
                status = random.choices(
                    ["OPERATIONAL", "MAINTENANCE", "OFFLINE"],
                    weights=status_weights
                )[0]

                if status != "OPERATIONAL":
                    current_usage = 0

                data.append({
                    "FACILITY_ID": f"FAC-{facility_id:04d}",
                    "FACILITY_NAME": f"{facility_type} {i + 1}",
                    "FACILITY_TYPE": facility_type,
                    "LOCATION": location,
                    "CAPACITY": capacity,
                    "CURRENT_USAGE": current_usage,
                    "OPERATING_HOURS": config["hours"],
                    "CONTACT_INFO": f"ext. {random.randint(2000, 9999)}",
                    "EQUIPMENT_LIST": config["equipment"],
                    "STATUS": status
                })
                facility_id += 1

                if facility_id > num_facilities:
                    break

            if facility_id > num_facilities:
                break

        return pd.DataFrame(data[:num_facilities])

    def generate_appointments(self, num_appointments: int = 200) -> pd.DataFrame:
        """Generate realistic appointment data linked to doctor schedules"""

        if not self.generated_schedules:
            raise ValueError("Must generate doctor schedules first!")

        # Generate appointments for the next 30 days
        start_date = date.today()
        date_range = [start_date + timedelta(days=x) for x in range(30)]

        appointment_statuses = ["SCHEDULED", "COMPLETED", "CANCELLED", "NO_SHOW"]
        status_weights = [0.60, 0.25, 0.10, 0.05]

        data = []
        appointment_id = 1

        # Create day name mapping
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        for _ in range(num_appointments):
            # Select a random date
            appointment_date = random.choice(date_range)
            day_of_week = day_names[appointment_date.weekday()]

            # Find schedules for this day
            available_schedules = [
                s for s in self.generated_schedules
                if s["day"] == day_of_week and s["booked"] < s["max_patients"]
            ]

            if not available_schedules:
                continue

            schedule = random.choice(available_schedules)

            # Generate appointment time within doctor's schedule
            # For simplicity, use hourly slots
            patient_id = f"PAT-{random.randint(1000, 9999):04d}"

            # Generate time within schedule range
            appointment_time = time(random.randint(8, 16), random.choice([0, 30]))

            # Status depends on date (past appointments more likely completed)
            if appointment_date < date.today():
                status = random.choices(
                    ["COMPLETED", "CANCELLED", "NO_SHOW"],
                    weights=[0.75, 0.15, 0.10]
                )[0]
            elif appointment_date == date.today():
                status = random.choices(
                    ["SCHEDULED", "COMPLETED", "CANCELLED"],
                    weights=[0.50, 0.40, 0.10]
                )[0]
            else:
                status = random.choices(
                    ["SCHEDULED", "CANCELLED"],
                    weights=[0.90, 0.10]
                )[0]

            data.append({
                "APPOINTMENT_ID": f"APT-{appointment_id:04d}",
                "PATIENT_ID": patient_id,
                "DOCTOR_ID": schedule["doctor_id"],
                "SCHEDULE_ID": schedule["schedule_id"],
                "APPOINTMENT_DATE": appointment_date,
                "APPOINTMENT_TIME": appointment_time,
                "STATUS": status,
                "CREATED_AT": datetime.now() - timedelta(days=random.randint(1, 60))
            })

            appointment_id += 1

        return pd.DataFrame(data)

    def generate_all_data(self) -> Dict[str, pd.DataFrame]:
        """Generate all hospital data with proper relationships"""

        print("Generating SOP data...")
        sop_df = self.generate_sop_data(num_records=50)

        print("Generating doctor schedules...")
        schedule_df = self.generate_doctor_schedule(num_doctors=25)

        print("Generating facility data...")
        facility_df = self.generate_facility_data(num_facilities=40)

        print("Generating appointments...")
        appointments_df = self.generate_appointments(num_appointments=200)

        return {
            "hospital_sop": sop_df,
            "doctor_schedule": schedule_df,
            "hospital_facilities": facility_df,
            "appointments": appointments_df
        }
