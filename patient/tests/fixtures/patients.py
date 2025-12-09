from sqlalchemy import text

def make_patient(
    nhs_number=None, first_name=None, last_name=None, dob=None,
    postcode=None, sex=None, verified=None
):
    patient = {
        "nhs_number": nhs_number,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "postcode": postcode,
        "sex": sex,
    }
    if verified is not None:
        patient["verified"] = verified
    return patient

def insert_patients(conn, patients):
    """Insert a list of patient dicts into the database and return their IDs."""
    ids = []
    for p in patients:
        result = conn.execute(text("""
            INSERT INTO patient (
                nhs_number, given_name, family_name, date_of_birth, 
                postcode, sex, verified, created_at, updated_at
            ) VALUES (
                :nhs_number, :first_name, :last_name, :dob,
                :postcode, :sex, :verified, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING patient_id
        """), p)
        ids.append(result.fetchone()[0])
    conn.commit()
    return ids